# journalflow

**Durable execution for LLM agents and backend workflows, in ~500 lines of Python on SQLite.**

Write a workflow as ordinary Python. Wrap anything with side effects (LLM calls, tool
calls, payments, DB writes) in `ctx.step(...)`. If the process crashes, gets
OOM-killed, or is redeployed mid-run, another worker picks the run up and **resumes at
the step where it stopped**. Completed steps are replayed from the journal, not
re-executed, so an agent that dies at step 9 doesn't pay for steps 1–8 again.

It is a small, readable version of the model behind Temporal, AWS Step Functions and
Azure Durable Functions. A chaos test kills real worker processes over a hundred times
per run, and the test fails unless every run still finishes correctly.

```
$ python examples/agent_demo.py --fresh --crash-after 4
  [executed] llm:plan               search: HNSW recall; search: vector DB filtering; ...
  [executed] tool:search[0]         2 results
  [executed] tool:search[1]         2 results
  !! simulated crash: process killed (os._exit, no cleanup) !!

$ python examples/agent_demo.py
  [replayed] llm:plan               search: HNSW recall; search: vector DB filtering; ...
  [replayed] tool:search[0]         2 results
  [replayed] tool:search[1]         2 results
  [replayed] tool:search[2]         2 results      <- committed just before the crash
  [executed] llm:draft              Draft: HNSW trades exactness for speed; ...
  [executed] llm:final              Final answer: ...
this process: 2 LLM calls, $0.1350
```

## Quickstart

```bash
pip install -e ".[dev]"
pytest                               # 21 tests incl. a multi-process chaos test, ~5 s
python chaos/run_chaos.py            # full chaos run, prints a JSON report
```

```python
from journalflow import Engine, RetryPolicy, StepFailed, idempotency_key

engine = Engine("app.db")

def charge(order_id, amount):
    # Same key on every retry *and* after a crash re-execution -> the PSP dedupes.
    return payments.charge(order_id, amount, idempotency_key=idempotency_key())

@engine.workflow()
def checkout(ctx, order_id, amount):
    ctx.step("reserve", inventory.reserve, order_id)
    try:
        receipt = ctx.step("charge", charge, order_id, amount,
                           retry=RetryPolicy(max_attempts=5, non_retryable=(CardDeclined,)))
    except StepFailed:
        ctx.step("release", inventory.release, order_id)    # saga-style compensation
        return {"status": "cancelled"}
    ctx.sleep(24 * 3600)                                     # durable: no thread held for a day
    ctx.step("feedback_email", mailer.send, order_id)
    return {"status": "done", "receipt": receipt}

run_id = engine.start("checkout", "order-42", 1999, run_id="order-42")  # idempotent start
engine.run_forever()     # any number of worker processes can share app.db
```

```bash
python -m journalflow --db app.db runs --status dead
python -m journalflow --db app.db show order-42      # run state + full journal
python -m journalflow --db app.db retry order-42     # after deploying a fix
```

## How it works

```
 worker A                         SQLite (WAL)                        worker B
 ────────                         ────────────                        ────────
 claim run ───────────────►  runs: lease_owner=A, epoch=1
 replay journal (empty)
 step "plan"  → LLM call
 append #0 (fenced, epoch=1) ─►  events: #0 plan = "..."
 step "search" → API call
 append #1 (fenced, epoch=1) ─►  events: #1 search = [...]
 💀 killed
                                 lease expires ...
                                 runs: lease_owner=B, epoch=2  ◄───── claim run
                                                                      replay: #0, #1 from journal
                                 events: #2 draft = "..."  ◄───────── step "draft" → LLM call
```

1. **Journal and replay.** Each run has an append-only event log in SQLite. A worker
   that claims a run calls the workflow function from the top. `ctx.step` number *n*
   checks the journal: if event *n* exists, it returns the recorded result without
   running the function. Otherwise it executes, journals the result and returns it.
   Workflow code must therefore be **deterministic outside steps**. `ctx.now()` and
   `ctx.uuid()` are journaled steps for exactly this reason.
2. **Leases with fencing tokens.** Claiming a run is an atomic `BEGIN IMMEDIATE`
   transaction that sets `lease_owner` and increments `lease_epoch`. Every journal
   append is `UPDATE runs ... WHERE lease_owner=? AND lease_epoch=?` plus the insert,
   in one transaction. A worker that stalls past its lease (GC pause, network partition)
   and wakes up later has its writes **rejected**. It cannot corrupt a run that another
   worker now owns. `PRIMARY KEY (run_id, seq)` enforces one outcome per step as a
   second line of defense.
3. **Retries** use exponential backoff with *full jitter*, so thousands of clients
   don't retry in sync against a recovering dependency. Errors marked non-retryable fail
   immediately. A step that exhausts its retries raises `StepFailed` *into the
   workflow*, which can catch it and compensate (saga pattern).
4. **Durable timers.** `ctx.sleep()` journals a wake-up time and releases the run, so no
   worker or thread is held. Any worker resumes the run once the time is due.
5. **Poison-pill dead-lettering.** A run whose previous worker died (its lease expired
   while status was `running`) gets a crash counter. After *N* crashes **with no
   journal progress in between**, the run moves to `dead` instead of crash-looping the
   fleet. `retry` re-queues it after a fix.
6. **Non-determinism detection.** If deployed code takes a different path than the
   journal records (a step renamed, reordered, or called with different arguments, by
   argument hash), the run fails loudly with `NonDeterminismError`. It doesn't silently
   mix two versions of the logic. That error derives from `BaseException`, so a
   careless `except Exception:` in workflow code can't swallow it.

## The guarantee, stated precisely

| | Guarantee | How |
|---|---|---|
| Journal entry per step | **exactly once** | fenced appends + `PRIMARY KEY (run_id, seq)` |
| Step's side effect | **at least once** | a crash between "effect happened" and "journal committed" re-runs the step |
| Effect, if the target honors `idempotency_key()` | **effectively once** | the key is `run_id/seq`: identical across retries and re-executions |

No system can promise exactly-once side effects against an arbitrary external API. If
the process dies after the API call returns but before the result is recorded, nobody
knows whether it happened. The honest contract is at-least-once plus idempotency keys,
the same contract Kafka consumers and Stripe's API use. The test
`test_crash_after_side_effect_repeats_step_with_same_idempotency_key` pins that window
down exactly.

## Chaos testing

`chaos/run_chaos.py` starts real worker processes that call `os._exit()` (no cleanup, no
`finally`, the equivalent of `kill -9`) at random points: before a step, **between a
side effect and its journal write**, and right after a journal write. Steps also fail
transiently 10 % of the time, and one poison workflow kills every worker that runs it.
A supervisor restarts dead workers until every run reaches a terminal state, then checks
the results:

```
python chaos/run_chaos.py --runs 100 --steps 8 --workers 6 --crash-prob 0.05
```

| metric | result |
|---|---|
| worker processes killed mid-flight | **134** (+5 by the poison run) |
| runs completed with correct result | **100 / 100** |
| journals gap-free, one outcome per step | **100 / 100** |
| side effects executed (raw) | 837 |
| side effects after idempotency-key dedup | **800 / 800 expected** |
| duplicate effects absorbed by keys | 37 |
| poison run | **dead-lettered**, queue kept flowing |
| wall time | 7.9 s |

Full report: [chaos/results.json](chaos/results.json). A smaller version runs in `pytest`
on every CI build.

### A bug the chaos test caught

The first version dead-lettered any run whose workers crashed 3 times in a row. Unit
tests passed. Under the chaos test at 5 % crash probability, **5 of 100 healthy runs
were dead-lettered**. They weren't poison; they were unlucky, crashing at different
steps while still making progress. The fix: reset the crash counter whenever a run
journals progress, so only runs that crash **at the same point** repeatedly count as
poison. `test_crashes_with_progress_in_between_are_not_poison` pins the regression. Three
consecutive heavy chaos runs pass after the fix.

## Tests

- **Deterministic unit tests** (`tests/test_engine.py`, 20 tests, <1 s): a fake clock
  controls leases and timers, and `SimulatedCrash` aborts a worker at an exact fault
  point. They cover:
  - replay does not re-execute steps
  - `now()`/`uuid()` are stable across replay
  - first-run vs. replay results have the same JSON shape
  - backoff stays within its jitter bounds
  - non-retryable errors fail immediately
  - saga compensation works
  - retry resumes from the failed step
  - a crash before or after the journal write is handled correctly
  - **a zombie worker is fenced off**: it stalls past its lease, another worker finishes the run, and the zombie's late write is rejected
  - poison runs are dead-lettered and don't block the queue
  - code and argument changes are detected as non-determinism
  - non-determinism can't be swallowed by `except Exception`
- **Chaos test** (`tests/test_chaos.py`): the multi-process run above, smaller.

## Design trade-offs and limitations

- **SQLite is the coordination layer.** It is great for one host and for clarity, and
  its single writer lock caps throughput (not yet benchmarked; the chaos run does
  ~800 steps in 8 s *including* 130+ crash recoveries with 1 s lease waits). The
  store is about 200 lines behind a narrow interface (`claim`, `append`, `release`,
  `renew`). A Postgres version would use `SELECT ... FOR UPDATE SKIP LOCKED`.
- **No lease heartbeat thread.** The lease is renewed before each attempt and on each
  journal write. A single step longer than `lease_ttl` lets another worker take over,
  and then fencing makes the slow worker's result lose. Set `lease_ttl` above your
  slowest step, or add a heartbeat.
- **Retry backoff blocks the worker** in-process. Long backoffs should become durable
  timers, as `ctx.sleep` already is.
- **No signals or queries** (e.g. "wait for human approval"). They are the natural next
  feature: an `inbox` table plus `ctx.wait_for_signal(name)` built on the timer
  machinery.
- **Workflow versioning** is detect-and-fail. Temporal-style `ctx.version("change-id")`
  branches would allow migrating in-flight runs.
- `synchronous=NORMAL` in WAL mode survives process crashes. An OS crash or power loss
  can roll back the last few commits, which is safe under the at-least-once contract.

## Layout

```
journalflow/engine.py    replay, steps, retries, timers, worker loop
journalflow/store.py     SQLite schema, leases, fenced writes, dead-lettering
journalflow/__main__.py  CLI: runs / show / retry
examples/agent_demo.py   crash-and-resume LLM agent demo (no API key needed)
chaos/                   multi-process chaos harness
tests/                   deterministic unit tests + chaos test
```
