"""Durable execution by journaling and deterministic replay.

A workflow is ordinary Python code. Anything with side effects or non-determinism
(API calls, LLM calls, DB writes, time, randomness) goes through ``ctx.step``,
which records the outcome in the journal before returning it. When a worker
crashes, another worker re-runs the workflow function from the top: completed steps
return their journaled results instantly instead of executing again, so the run
resumes exactly where it stopped.

Delivery guarantee: a step's *journal entry* is written exactly once (fenced, and
enforced by a primary key). The step's *side effect* is at-least-once: if a worker
dies after the effect but before the journal commit, the step runs again. Steps
get a stable :func:`idempotency_key` so external systems can deduplicate. This is
the same contract Temporal, Step Functions, and Kafka consumers give you.
"""

from __future__ import annotations

import contextvars
import hashlib
import json
import logging
import os
import random
import socket
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

from .store import TERMINAL, Event, Lease, LeaseLost, Store

log = logging.getLogger("journalflow")


# ------------------------------------------------------------------------ errors


class StepFailed(Exception):
    """A step exhausted its retries. Workflows may catch it (e.g. to compensate)."""

    def __init__(self, step: str, error_type: str, message: str) -> None:
        super().__init__(f"step {step!r} failed: {error_type}: {message}")
        self.step, self.error_type, self.message = step, error_type, message


class NonDeterminismError(BaseException):
    """Workflow code took a different path on replay than the journal records --
    usually a code change deployed while runs were in flight. BaseException so a
    broad ``except Exception`` in workflow code cannot hide it."""


class SimulatedCrash(BaseException):
    """Raised by fault hooks in tests to model a worker dying at a precise point."""


class _Suspend(BaseException):
    def __init__(self, wake_at: float) -> None:
        self.wake_at = wake_at


# ------------------------------------------------------------------ retry policy


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    initial_delay: float = 0.1
    multiplier: float = 2.0
    max_delay: float = 10.0
    non_retryable: tuple[type[BaseException], ...] = ()

    def delay(self, attempt: int, rng: random.Random) -> float:
        """Exponential backoff with *full jitter* (uniform in [0, cap]) so that many
        clients failing at once don't retry in lockstep against a recovering service."""
        cap = min(self.max_delay, self.initial_delay * self.multiplier ** (attempt - 1))
        return rng.uniform(0, cap)


# --------------------------------------------------------- step-local context


@dataclass(frozen=True)
class StepInfo:
    run_id: str
    seq: int
    name: str
    attempt: int

    @property
    def idempotency_key(self) -> str:
        return f"{self.run_id}/{self.seq}"


_current_step: contextvars.ContextVar[StepInfo | None] = contextvars.ContextVar("step", default=None)


def current_step() -> StepInfo:
    """Inside a step function: which run/step/attempt is executing."""
    info = _current_step.get()
    if info is None:
        raise RuntimeError("current_step() called outside a step")
    return info


def idempotency_key() -> str:
    """Stable across retries *and* crash re-executions of the same step. Pass it to
    payment APIs, ``INSERT ... ON CONFLICT DO NOTHING``, message dedup headers, etc."""
    return current_step().idempotency_key


def _args_digest(args: tuple, kwargs: dict) -> str:
    blob = json.dumps([args, kwargs], sort_keys=True, default=repr)
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


# --------------------------------------------------------------- workflow context


class WorkflowContext:
    def __init__(self, engine: "Engine", lease: Lease, history: list[Event]) -> None:
        self._engine = engine
        self._lease = lease
        self._history = {e.seq: e for e in history}
        self._seq = 0
        self.run_id = lease.run_id

    @property
    def is_replaying(self) -> bool:
        """True while re-executing already-journaled steps (use to silence logging)."""
        return self._seq in self._history

    def step(self, name: str, fn: Callable[..., Any], *args: Any,
             retry: RetryPolicy | None = None, **kwargs: Any) -> Any:
        """Run ``fn(*args, **kwargs)`` durably. Its JSON-serializable return value is
        journaled; on replay it is returned without calling ``fn`` again."""
        seq = self._seq
        self._seq += 1
        digest = _args_digest(args, kwargs)
        recorded = self._history.get(seq)

        if recorded is not None:
            if recorded.kind not in ("step", "step_failed") or recorded.name != name:
                raise NonDeterminismError(
                    f"run {self.run_id} seq {seq}: code called step {name!r}, "
                    f"journal has {recorded.kind} {recorded.name!r}")
            if recorded.payload["args"] != digest:
                raise NonDeterminismError(
                    f"run {self.run_id} step {name!r}: arguments differ from the journaled call")
            log.debug("replay %s #%d %s", self.run_id, seq, name)
            if recorded.kind == "step":
                return recorded.payload["result"]
            p = recorded.payload
            raise StepFailed(name, p["error_type"], p["message"])

        engine = self._engine
        policy = retry or engine.default_retry
        attempt = 0
        while True:
            attempt += 1
            info = StepInfo(self.run_id, seq, name, attempt)
            engine._renew(self._lease)
            engine._fault("before_step", info)
            token = _current_step.set(info)
            try:
                result = fn(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001 -- user code may raise anything
                retryable = not isinstance(exc, policy.non_retryable)
                if retryable and attempt < policy.max_attempts:
                    pause = policy.delay(attempt, engine._rng)
                    log.info("step %s #%d %s attempt %d failed (%r); retry in %.2fs",
                             self.run_id, seq, name, attempt, exc, pause)
                    engine.sleep(pause)
                    continue
                payload = {"args": digest, "error_type": type(exc).__name__,
                           "message": str(exc), "attempts": attempt}
                self._journal(seq, "step_failed", name, payload)
                raise StepFailed(name, type(exc).__name__, str(exc)) from exc
            finally:
                _current_step.reset(token)

            engine._fault("after_side_effect", info)  # the at-least-once window
            try:
                # Round-trip through JSON so the first execution returns exactly what
                # every later replay will (tuples become lists, int keys become str...).
                result = json.loads(json.dumps(result))
            except TypeError as exc:
                raise TypeError(f"step {name!r} must return JSON-serializable data: {exc}") from None
            self._journal(seq, "step", name, {"args": digest, "result": result, "attempts": attempt})
            engine._fault("after_journal", info)
            log.info("step %s #%d %s done (attempt %d)", self.run_id, seq, name, attempt)
            return result

    def sleep(self, seconds: float) -> None:
        """Durable timer. The worker is released while the run waits, so a run can
        sleep for days without holding a thread, and survives restarts."""
        seq = self._seq
        self._seq += 1
        recorded = self._history.get(seq)
        if recorded is None:
            wake_at = self._engine.clock() + seconds
            self._journal(seq, "timer", "sleep", {"wake_at": wake_at})
        elif recorded.kind != "timer":
            raise NonDeterminismError(
                f"run {self.run_id} seq {seq}: code called sleep, journal has {recorded.kind} {recorded.name!r}")
        else:
            wake_at = recorded.payload["wake_at"]
        if self._engine.clock() < wake_at:
            raise _Suspend(wake_at)

    def now(self) -> float:
        """Deterministic wall-clock: journaled on first call, replayed afterwards."""
        return self.step("now", self._engine.clock)

    def uuid(self) -> str:
        """Deterministic UUID: journaled on first call, replayed afterwards."""
        return self.step("uuid", lambda: uuid.uuid4().hex)

    def _journal(self, seq: int, kind: str, name: str, payload: dict) -> None:
        e = self._engine
        e.store.append(self._lease, seq, kind, name, payload, e.clock(), e.lease_ttl)


# ------------------------------------------------------------------------ engine


@dataclass
class Engine:
    """Client API and worker in one object. Run one per process; any number of
    processes may share the same database file.

    ``clock``/``sleep`` are injectable so tests can control time; ``faults`` is a
    hook called at named points (``before_step``, ``after_side_effect``,
    ``after_journal``) for fault-injection testing.
    """

    db_path: str
    worker_id: str = field(default_factory=lambda: f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}")
    lease_ttl: float = 30.0
    max_crash_claims: int = 3
    default_retry: RetryPolicy = field(default_factory=RetryPolicy)
    clock: Callable[[], float] = time.time
    sleep: Callable[[float], None] = time.sleep
    faults: Callable[[str, StepInfo], None] | None = None
    seed: int | None = None

    def __post_init__(self) -> None:
        self.store = Store(self.db_path)
        self._workflows: dict[str, Callable[..., Any]] = {}
        self._rng = random.Random(self.seed)

    def close(self) -> None:
        self.store.close()

    # --------------------------------------------------------------- registry

    def workflow(self, name: str | None = None) -> Callable:
        """Decorator: ``@engine.workflow()`` registers ``fn(ctx, *args)``."""
        def register(fn: Callable) -> Callable:
            self._workflows[name or fn.__name__] = fn
            return fn
        return register

    # ----------------------------------------------------------------- client

    def start(self, workflow: str, *args: Any, run_id: str | None = None) -> str:
        """Enqueue a run. Passing your own ``run_id`` makes start idempotent:
        a retried request with the same id returns the existing run."""
        run_id = run_id or uuid.uuid4().hex
        json.dumps(args)  # fail fast on non-JSON input
        self.store.create_run(run_id, workflow, list(args), self.clock())
        return run_id

    def get_run(self, run_id: str) -> dict:
        run = self.store.get_run(run_id)
        if run is None:
            raise KeyError(run_id)
        return run

    def result(self, run_id: str) -> Any:
        run = self.get_run(run_id)
        if run["status"] != "completed":
            raise RuntimeError(f"run {run_id} is {run['status']}: {run['error']}")
        return run["result"]

    def history(self, run_id: str) -> list[Event]:
        return self.store.events(run_id)

    def retry(self, run_id: str) -> None:
        """Re-queue a failed or dead run (e.g. after deploying a fix). Completed
        steps are not re-executed; a trailing failed step is attempted again."""
        self.store.reset_for_retry(run_id, self.clock())

    # ----------------------------------------------------------------- worker

    def run_once(self) -> bool:
        """Claim and advance one ready run. Returns False if nothing was ready."""
        lease = self.store.claim(list(self._workflows), self.worker_id, self.clock(),
                                 self.lease_ttl, self.max_crash_claims)
        if lease is None:
            return False
        self._execute(lease)
        return True

    def drain(self) -> int:
        """Process runs until none is ready right now. Returns how many were advanced."""
        n = 0
        while self.run_once():
            n += 1
        return n

    def run_forever(self, poll_interval: float = 0.2, should_stop: Callable[[], bool] = lambda: False) -> None:
        while not should_stop():
            if not self.run_once():
                time.sleep(poll_interval)

    def wait(self, run_id: str, timeout: float = 60.0, poll_interval: float = 0.05) -> dict:
        """Work runs until ``run_id`` reaches a terminal state; return it."""
        deadline = time.monotonic() + timeout
        while (run := self.get_run(run_id))["status"] not in TERMINAL:
            if time.monotonic() > deadline:
                raise TimeoutError(f"run {run_id} still {run['status']}")
            if not self.run_once():
                time.sleep(poll_interval)
        return run

    def _execute(self, lease: Lease) -> None:
        fn = self._workflows[lease.workflow]
        ctx = WorkflowContext(self, lease, self.store.events(lease.run_id))
        now = self.clock
        try:
            try:
                result = fn(ctx, *lease.input)
            except _Suspend as s:
                self.store.release(lease, "sleeping", now(), wake_at=s.wake_at)
                return
            except NonDeterminismError as exc:
                log.error("non-determinism in %s: %s", lease.run_id, exc)
                self.store.release(lease, "failed", now(), error=f"NonDeterminismError: {exc}")
                return
            except Exception as exc:  # noqa: BLE001
                self.store.release(lease, "failed", now(), error=f"{type(exc).__name__}: {exc}")
                return
            try:
                json.dumps(result)
            except TypeError as exc:
                self.store.release(lease, "failed", now(), error=f"workflow result not JSON-serializable: {exc}")
                return
            self.store.release(lease, "completed", now(), result=result)
        except LeaseLost as exc:
            # Another worker owns this run now; everything we did is either already
            # journaled (and will be replayed) or was rejected by the fence.
            log.warning("abandoning run: %s", exc)

    def _renew(self, lease: Lease) -> None:
        self.store.renew(lease, self.clock(), self.lease_ttl)

    def _fault(self, point: str, info: StepInfo) -> None:
        if self.faults is not None:
            self.faults(point, info)
