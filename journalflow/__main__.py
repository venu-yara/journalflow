"""Inspect and operate a journal: ``python -m journalflow --db app.db runs|show|retry``."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime

from .store import Store


def _ts(t: float | None) -> str:
    return "-" if t is None else datetime.fromtimestamp(t).strftime("%Y-%m-%d %H:%M:%S")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m journalflow")
    p.add_argument("--db", required=True)
    sub = p.add_subparsers(dest="cmd", required=True)
    runs = sub.add_parser("runs", help="list runs")
    runs.add_argument("--status")
    show = sub.add_parser("show", help="show one run and its journal")
    show.add_argument("run_id")
    retry = sub.add_parser("retry", help="re-queue a failed or dead run")
    retry.add_argument("run_id")
    args = p.parse_args(argv)

    store = Store(args.db)
    if args.cmd == "runs":
        for r in store.list_runs(args.status):
            print(f"{r['run_id']:<34} {r['workflow']:<20} {r['status']:<10} {_ts(r['updated_at'])}  {r['error'] or ''}")
    elif args.cmd == "show":
        run = store.get_run(args.run_id)
        if run is None:
            print(f"no run {args.run_id}", file=sys.stderr)
            return 1
        for key in ("workflow", "status", "input", "result", "error", "lease_owner", "lease_epoch", "crash_claims"):
            print(f"{key:>13}: {run[key]}")
        print()
        for e in store.events(args.run_id):
            body = {k: v for k, v in e.payload.items() if k != "args"}
            print(f"  #{e.seq:<3} epoch {e.epoch:<3} {e.kind:<11} {e.name:<24} {json.dumps(body)[:80]}")
    elif args.cmd == "retry":
        store.reset_for_retry(args.run_id, datetime.now().timestamp())
        print(f"re-queued {args.run_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
