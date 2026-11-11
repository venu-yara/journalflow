"""Deterministic, in-process tests. Time is a fake clock; crashes are injected at
exact points with SimulatedCrash, which aborts a worker mid-flight without any
cleanup -- the same state a killed process leaves behind."""

import pytest

from journalflow import (
    Engine,
    NonDeterminismError,
    RetryPolicy,
    SimulatedCrash,
    StepFailed,
    current_step,
    idempotency_key,
)


class FakeClock:
    def __init__(self, t=1_000.0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, seconds):
        self.t += seconds


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def make(tmp_path, clock):
    engines = []

    def factory(worker_id="w1", **kw):
        kw.setdefault("lease_ttl", 10.0)
        kw.setdefault("sleep", lambda s: None)
        e = Engine(str(tmp_path / "jf.db"), worker_id=worker_id, clock=clock, seed=0, **kw)
        engines.append(e)
        return e

    yield factory
    for e in engines:
        e.close()


class Counter:
    """A fake external system that records every call it receives."""

    def __init__(self):
        self.calls = []

    def __call__(self, *args):
        self.calls.append((args, current_step().idempotency_key))
        return list(args)


def crash_once_at(point, step_name=None):
    fired = []

    def hook(p, info):
        if p == point and not fired and (step_name is None or info.name == step_name):
            fired.append(info)
            raise SimulatedCrash(f"{p} in {info.name}")

    return hook


# ---------------------------------------------------------------- happy path


def test_runs_steps_and_records_result(make):
    e = make()
    a, b = Counter(), Counter()

    @e.workflow()
    def wf(ctx, x):
        return ctx.step("a", a, x) + ctx.step("b", b, x + 1)

    rid = e.start("wf", 1)
    assert e.drain() == 1
    assert e.result(rid) == [1, 2]
    assert len(a.calls) == len(b.calls) == 1
    assert [(ev.seq, ev.kind, ev.name) for ev in e.history(rid)] == [(0, "step", "a"), (1, "step", "b")]


def test_idempotent_start(make):
    e = make()
    e.workflow("wf")(lambda ctx: 1)
    assert e.start("wf", run_id="order-42") == e.start("wf", run_id="order-42") == "order-42"
    assert len(e.store.list_runs()) == 1


# ------------------------------------------------------- replay and timers


def test_sleep_releases_worker_and_replay_skips_completed_steps(make, clock):
    e = make()
    a, b = Counter(), Counter()

    @e.workflow()
    def wf(ctx):
        ctx.step("a", a)
        ctx.sleep(3600)
        return ctx.step("b", b)

    rid = e.start("wf")
    e.drain()
    run = e.get_run(rid)
    assert run["status"] == "sleeping" and run["lease_owner"] is None
    assert e.drain() == 0, "not ready before wake time"

    clock.advance(3600)
    e.drain()
    assert e.get_run(rid)["status"] == "completed"
    assert len(a.calls) == 1, "completed step must be replayed, not re-executed"
    assert len(b.calls) == 1


def test_now_and_uuid_are_stable_across_replay(make, clock):
    e = make()
    seen = []

    @e.workflow()
    def wf(ctx):
        seen.append((ctx.now(), ctx.uuid()))
        ctx.sleep(10)
        seen.append((ctx.now(), ctx.uuid()))
        return seen[-1]

    rid = e.start("wf")
    e.drain()
    clock.advance(10)
    e.drain()
    # 3 executions of the function body prefix: first run, replay (2 values each)
    assert seen[0] == seen[1], "replayed now()/uuid() must equal the originals"
    assert seen[2][0] == clock() and seen[2] != seen[1]
    assert e.result(rid) == list(seen[2])


def test_first_run_returns_same_shape_as_replay(make, clock):
    e = make()
    shapes = []

    @e.workflow()
    def wf(ctx):
        r = ctx.step("tuple", lambda: (1, {2: "x"}))
        shapes.append(r)
        ctx.sleep(1)
        return r

    e.start("wf")
    e.drain()
    clock.advance(1)
    e.drain()
    assert shapes[0] == shapes[1] == [1, {"2": "x"}]


# --------------------------------------------------------------- retries


def test_retries_with_backoff_then_succeeds(make):
    delays = []
    e = make(sleep=delays.append,
             default_retry=RetryPolicy(max_attempts=5, initial_delay=1, multiplier=2, max_delay=3))
    attempts = []

    def flaky():
        attempts.append(current_step().attempt)
        if len(attempts) < 4:
            raise ConnectionError("upstream 503")
        return "ok"

    e.workflow("wf")(lambda ctx: ctx.step("call", flaky))
    rid = e.start("wf")
    e.drain()
    assert e.result(rid) == "ok"
    assert attempts == [1, 2, 3, 4]
    caps = [1, 2, 3]  # 1, 2, min(4, max_delay=3)
    assert len(delays) == 3 and all(0 <= d <= c for d, c in zip(delays, caps))
    assert e.history(rid)[0].payload["attempts"] == 4


def test_non_retryable_error_fails_immediately(make):
    e = make()
    calls = []

    def bad():
        calls.append(1)
        raise ValueError("invalid card")

    e.workflow("wf")(lambda ctx: ctx.step(
        "charge", bad, retry=RetryPolicy(max_attempts=5, non_retryable=(ValueError,))))
    rid = e.start("wf")
    e.drain()
    run = e.get_run(rid)
    assert run["status"] == "failed" and "invalid card" in run["error"]
    assert len(calls) == 1


def test_step_failure_can_be_compensated_saga(make):
    e = make(default_retry=RetryPolicy(max_attempts=2))
    released = Counter()

    def ship():
        raise RuntimeError("carrier down")

    @e.workflow()
    def order(ctx):
        ctx.step("reserve", lambda: "held")
        try:
            ctx.step("ship", ship)
        except StepFailed as f:
            ctx.step("release", released, "reserve")
            return {"status": "cancelled", "reason": f.message}
        return {"status": "shipped"}

    rid = e.start("order")
    e.drain()
    assert e.result(rid) == {"status": "cancelled", "reason": "carrier down"}
    assert len(released.calls) == 1
    assert [ev.kind for ev in e.history(rid)] == ["step", "step_failed", "step"]


def test_retry_failed_run_resumes_from_failed_step(make):
    e = make(default_retry=RetryPolicy(max_attempts=1))
    first = Counter()
    broken = {"on": True}

    def second():
        if broken["on"]:
            raise RuntimeError("bug")
        return "fixed"

    @e.workflow()
    def wf(ctx):
        ctx.step("first", first)
        return ctx.step("second", second)

    rid = e.start("wf")
    e.drain()
    assert e.get_run(rid)["status"] == "failed"
    broken["on"] = False  # "deploy the fix"
    e.retry(rid)
    e.drain()
    assert e.result(rid) == "fixed"
    assert len(first.calls) == 1


# ------------------------------------------------------- crashes and leases


def test_crash_after_journal_does_not_repeat_step(make, clock):
    a = Counter()

    def wf(ctx):
        ctx.step("a", a)
        return ctx.step("b", lambda: "done")

    w1 = make("w1", faults=crash_once_at("after_journal", "a"))
    w1.workflow("wf")(wf)
    rid = w1.start("wf")
    with pytest.raises(SimulatedCrash):
        w1.drain()

    w2 = make("w2")
    w2.workflow("wf")(wf)
    assert w2.drain() == 0, "lease still held by the dead worker"
    clock.advance(10)
    w2.drain()
    assert w2.result(rid) == "done"
    assert len(a.calls) == 1


def test_crash_after_side_effect_repeats_step_with_same_idempotency_key(make, clock):
    """The honest at-least-once window: effect happened, journal write didn't."""
    a = Counter()

    def wf(ctx):
        return ctx.step("charge", a, 100)

    w1 = make("w1", faults=crash_once_at("after_side_effect"))
    w1.workflow("wf")(wf)
    rid = w1.start("wf")
    with pytest.raises(SimulatedCrash):
        w1.drain()
    clock.advance(10)
    w2 = make("w2")
    w2.workflow("wf")(wf)
    w2.drain()

    assert w2.result(rid) == [100]
    assert len(a.calls) == 2, "side effect executed twice"
    keys = {k for _, k in a.calls}
    assert keys == {f"{rid}/0"}, "but both executions carried the same idempotency key"
    assert len(w2.history(rid)) == 1, "and the journal recorded exactly one outcome"


def test_zombie_worker_is_fenced_off(make, clock):
    """w1 stalls mid-step past its lease; w2 takes over and finishes. When w1 wakes
    up, its journal write must be rejected rather than corrupting the run."""
    w2 = make("w2")
    w1 = make("w1")
    calls = []

    def slow_step():
        calls.append(len(calls))
        if len(calls) == 1:  # w1's call: stall long enough to lose the lease
            clock.advance(60)
            assert w2.drain() == 1
        return f"call-{len(calls)}"

    for w in (w1, w2):
        w.workflow("wf")(lambda ctx: ctx.step("slow", slow_step))

    rid = w1.start("wf")
    w1.drain()  # returns normally: w1 logs LeaseLost and abandons the run

    run = w1.get_run(rid)
    assert run["status"] == "completed" and run["result"] == "call-2"
    hist = w1.history(rid)
    assert len(hist) == 1 and hist[0].epoch == 2, "only the new owner's write landed"


def test_poison_run_is_dead_lettered(make, clock):
    def always_crash(point, info):
        if point == "before_step":
            raise SimulatedCrash("segfault in native lib")

    e = make(faults=always_crash, max_crash_claims=3)
    e.workflow("wf")(lambda ctx: ctx.step("boom", lambda: None))
    healthy = make("w2", max_crash_claims=3)
    healthy.workflow("ok")(lambda ctx: "fine")
    rid = e.start("wf")

    for _ in range(3):
        with pytest.raises(SimulatedCrash):
            e.drain()
        clock.advance(10)
    assert e.drain() == 0
    run = e.get_run(rid)
    assert run["status"] == "dead" and "crashed 3 times" in run["error"]

    other = healthy.start("ok")
    healthy.drain()
    assert healthy.result(other) == "fine", "dead run doesn't block the queue"


def test_crashes_with_progress_in_between_are_not_poison(make, clock):
    """Regression test for a bug the chaos test found: a healthy run that was
    unlucky enough to see 3+ worker crashes was dead-lettered as poison."""
    crashes = []

    def crash_after_each_step(point, info):
        if point == "after_journal" and info.seq not in crashes:
            crashes.append(info.seq)
            raise SimulatedCrash()

    e = make(faults=crash_after_each_step, max_crash_claims=2)

    @e.workflow()
    def wf(ctx):
        return sum(ctx.step(f"s{i}", lambda i=i: i) for i in range(5))

    rid = e.start("wf")
    for _ in range(5):
        with pytest.raises(SimulatedCrash):
            e.drain()
        clock.advance(10)
    e.drain()
    assert len(crashes) == 5
    assert e.result(rid) == 10


def test_worker_only_claims_workflows_it_knows(make):
    a = make("a")
    a.workflow("known")(lambda ctx: 1)
    rid = a.start("unknown-to-a")
    assert a.drain() == 0
    assert a.get_run(rid)["status"] == "pending"


# -------------------------------------------------------- determinism checks


def test_code_change_mid_flight_is_detected(make, clock):
    v1 = make("v1")

    @v1.workflow("wf")
    def wf_v1(ctx):
        ctx.step("fetch", lambda: 1)
        ctx.sleep(5)
        return "v1"

    rid = v1.start("wf")
    v1.drain()

    v2 = make("v2")

    @v2.workflow("wf")
    def wf_v2(ctx):
        ctx.step("fetch_v2", lambda: 1)  # renamed step: history no longer matches
        ctx.sleep(5)
        return "v2"

    clock.advance(5)
    v2.drain()
    run = v2.get_run(rid)
    assert run["status"] == "failed" and run["error"].startswith("NonDeterminismError")


def test_changed_step_arguments_are_detected(make, clock):
    e = make()
    arg = {"v": 1}

    @e.workflow()
    def wf(ctx):
        ctx.step("s", lambda x: x, arg["v"])
        ctx.sleep(1)

    rid = e.start("wf")
    e.drain()
    arg["v"] = 2
    clock.advance(1)
    e.drain()
    assert "arguments differ" in e.get_run(rid)["error"]


def test_nondeterminism_cannot_be_swallowed_by_broad_except(make, clock):
    e = make()
    version = {"n": 1}

    @e.workflow()
    def wf(ctx):
        try:
            ctx.step(f"s{version['n']}", lambda: None)
        except Exception:  # noqa: BLE001
            return "swallowed"
        ctx.sleep(1)
        return "ok"

    rid = e.start("wf")
    e.drain()
    version["n"] = 2
    clock.advance(1)
    e.drain()
    assert e.get_run(rid)["status"] == "failed"


def test_non_json_step_result_fails_run(make):
    e = make()
    e.workflow("wf")(lambda ctx: ctx.step("bad", lambda: object()))
    rid = e.start("wf")
    e.drain()
    assert "JSON-serializable" in e.get_run(rid)["error"]


def test_idempotency_key_outside_step_raises():
    with pytest.raises(RuntimeError):
        idempotency_key()
