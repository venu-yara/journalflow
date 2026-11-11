"""Workflows and a fake "external system" used by the chaos test.

The sink is a separate SQLite database standing in for a payments API or a
downstream service. Every step writes to it twice:
  * ``effects_raw``  -- plain INSERT, like a naive API without idempotency support
  * ``effects_once`` -- INSERT keyed by the step's idempotency key (deduplicated)
Comparing the two tables shows exactly how many duplicate side effects crashes
caused, and that idempotency keys absorb all of them.
"""

from __future__ import annotations

import os
import random
import sqlite3
import time

from journalflow import Engine, RetryPolicy, current_step

SINK_SCHEMA = """
CREATE TABLE IF NOT EXISTS effects_raw  (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, step INTEGER, pid INTEGER);
CREATE TABLE IF NOT EXISTS effects_once (key TEXT PRIMARY KEY, run_id TEXT, step INTEGER);
"""


def init_sink(path: str) -> None:
    with sqlite3.connect(path) as c:
        c.execute("PRAGMA journal_mode=WAL")
        c.executescript(SINK_SCHEMA)


def write_effect(sink: str, step_no: int, flaky: float) -> str:
    info = current_step()
    if random.random() < flaky:  # transient upstream error -> exercises retries
        raise ConnectionError("sink unavailable (injected)")
    time.sleep(0.005)
    with sqlite3.connect(sink, timeout=30) as c:
        c.execute("INSERT INTO effects_raw (run_id, step, pid) VALUES (?, ?, ?)",
                  (info.run_id, step_no, os.getpid()))
        c.execute("INSERT OR IGNORE INTO effects_once (key, run_id, step) VALUES (?, ?, ?)",
                  (info.idempotency_key, info.run_id, step_no))
    return info.idempotency_key


def explode() -> None:
    os._exit(13)  # a step that kills its worker every time: a poison pill


def build(db: str, **kw) -> Engine:
    # At chaos crash rates (~15% chance a worker dies on any given step), 3 deaths on
    # the *same* step happen by luck about once per 300 steps, so allow 5 here.
    # Real fleets crash orders of magnitude less often; the default of 3 is fine.
    kw.setdefault("max_crash_claims", 5)
    engine = Engine(db, default_retry=RetryPolicy(max_attempts=10, initial_delay=0.01, max_delay=0.1), **kw)

    @engine.workflow()
    def pipeline(ctx, sink: str, n_steps: int, flaky: float):
        keys = [ctx.step(f"effect-{i}", write_effect, sink, i, flaky) for i in range(n_steps // 2)]
        ctx.sleep(0.2)  # durable timer in the middle: run is released and re-claimed
        keys += [ctx.step(f"effect-{i}", write_effect, sink, i, flaky) for i in range(n_steps // 2, n_steps)]
        return len(keys)

    @engine.workflow()
    def poison(ctx):
        ctx.step("explode", explode)

    return engine
