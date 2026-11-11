"""Chaos test: real worker processes, killed at random, must still finish every run.

A supervisor keeps N worker processes alive. Each worker exits abruptly at random
fault points (before a step, between a side effect and its journal write, right
after a journal write). Steps also fail transiently at random. One poison run kills
every worker that touches it.

Pass criteria:
  1. every normal run completes with the right result
  2. the poison run is dead-lettered instead of crash-looping forever
  3. every run's journal is gap-free (seq 0..n), one outcome per step
  4. deduplicated side effects == runs x steps exactly; raw side effects >= that

Usage: python chaos/run_chaos.py [--runs 30 --steps 6 --workers 4 --crash-prob 0.03]
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE))

import app  # noqa: E402
from worker import CRASH_EXIT  # noqa: E402

from journalflow.store import TERMINAL  # noqa: E402


def run(runs=30, steps=6, workers=4, crash_prob=0.03, flaky=0.1, lease_ttl=1.0, timeout=180.0,
        workdir: str | None = None) -> dict:
    tmp = Path(workdir or tempfile.mkdtemp(prefix="journalflow-chaos-"))
    db, sink = str(tmp / "journal.db"), str(tmp / "sink.db")
    app.init_sink(sink)
    client = app.build(db)
    run_ids = [client.start("pipeline", sink, steps, flaky) for _ in range(runs)]
    poison_id = client.start("poison")

    cmd = [sys.executable, str(HERE / "worker.py"), db, str(crash_prob), str(lease_ttl)]
    procs = [subprocess.Popen(cmd) for _ in range(workers)]
    exits: dict[int, int] = {}
    t0 = time.monotonic()
    try:
        while True:
            for i, p in enumerate(procs):
                code = p.poll()
                if code is not None:  # worker died: count it and replace it
                    exits[code] = exits.get(code, 0) + 1
                    procs[i] = subprocess.Popen(cmd)
            statuses = [client.get_run(r)["status"] for r in run_ids + [poison_id]]
            if all(s in TERMINAL for s in statuses):
                break
            if time.monotonic() - t0 > timeout:
                raise TimeoutError(f"not finished after {timeout}s: {statuses}")
            time.sleep(0.05)
    finally:
        for p in procs:
            p.kill()
            p.wait()
    elapsed = time.monotonic() - t0

    # ------------------------------------------------------------- verification
    failures = []
    for r in run_ids:
        info = client.get_run(r)
        if info["status"] != "completed" or info["result"] != steps:
            failures.append(f"run {r}: {info['status']} {info['error']}")
        seqs = [e.seq for e in client.history(r)]
        if seqs != list(range(steps + 1)):  # n steps + 1 timer
            failures.append(f"run {r}: journal seqs {seqs}")
    poison = client.get_run(poison_id)
    if poison["status"] != "dead":
        failures.append(f"poison run is {poison['status']}, expected dead")

    with sqlite3.connect(sink) as c:
        raw = c.execute("SELECT COUNT(*) FROM effects_raw").fetchone()[0]
        once = c.execute("SELECT COUNT(*) FROM effects_once").fetchone()[0]
    if once != runs * steps:
        failures.append(f"deduplicated effects {once} != {runs * steps}")
    epochs = client.store.conn.execute(
        "SELECT COALESCE(SUM(lease_epoch), 0) FROM runs").fetchone()[0]
    client.close()

    return {
        "config": {"runs": runs, "steps": steps, "workers": workers, "crash_prob": crash_prob,
                   "flaky": flaky, "lease_ttl": lease_ttl},
        "seconds": round(elapsed, 1),
        "worker_crashes_injected": exits.get(CRASH_EXIT, 0),
        "worker_crashes_poison": exits.get(13, 0),
        "other_worker_exits": {k: v for k, v in exits.items() if k not in (CRASH_EXIT, 13)},
        "lease_claims": epochs,
        "side_effects_expected": runs * steps,
        "side_effects_raw": raw,
        "side_effects_deduplicated": once,
        "duplicates_absorbed_by_idempotency_keys": raw - once,
        "poison_status": poison["status"],
        "failures": failures,
        "passed": not failures,
    }


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--runs", type=int, default=30)
    p.add_argument("--steps", type=int, default=6)
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--crash-prob", type=float, default=0.03)
    p.add_argument("--flaky", type=float, default=0.1)
    p.add_argument("--lease-ttl", type=float, default=1.0)
    p.add_argument("--save", action="store_true", help="write chaos/results.json")
    args = p.parse_args()
    report = run(args.runs, args.steps, args.workers, args.crash_prob, args.flaky, args.lease_ttl)
    print(json.dumps(report, indent=2))
    if args.save:
        (HERE / "results.json").write_text(json.dumps(report, indent=2) + "\n")
    sys.exit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
