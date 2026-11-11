"""Crash an LLM agent mid-run, restart it, and watch it resume without re-paying
for the LLM calls it already made.

    python examples/agent_demo.py --crash-after 4   # dies abruptly after 4 steps
    python examples/agent_demo.py                   # resumes from step 5

The "LLM" is a scripted fake so the demo needs no API key; each call sleeps to
mimic latency and is billed at a made-up rate. Swapping in a real client only
changes ``fake_llm`` -- the durability comes from ``ctx.step``.
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from journalflow import Engine, RetryPolicy  # noqa: E402

DB = Path(__file__).with_name("agent_demo.db")
spent = {"llm_calls": 0, "usd": 0.0}


def fake_llm(prompt: str, tokens: int) -> str:
    time.sleep(0.4)
    spent["llm_calls"] += 1
    spent["usd"] += tokens * 15 / 1_000_000
    if prompt.startswith("plan"):
        return "search: HNSW recall; search: vector DB filtering; search: pgvector benchmarks"
    if prompt.startswith("draft"):
        return "Draft: HNSW trades exactness for speed; filtered search is the hard part..."
    return "Final answer: use HNSW with pre-filter partitions for high-selectivity filters."


def web_search(query: str) -> list[str]:
    if random.random() < 0.3:
        raise TimeoutError("search API timed out")  # transient: retried with backoff
    time.sleep(0.15)
    return [f"https://example.org/{query.replace(' ', '-')}/{i}" for i in range(2)]


def build(crash_after: int | None) -> Engine:
    done = {"n": 0}

    def maybe_crash(point, info):
        if point == "after_journal":
            done["n"] += 1
            if crash_after is not None and done["n"] >= crash_after:
                print("\n  !! simulated crash: process killed (os._exit, no cleanup) !!\n")
                os._exit(1)

    engine = Engine(str(DB), faults=maybe_crash, lease_ttl=1.0,
                    default_retry=RetryPolicy(max_attempts=5, initial_delay=0.2))

    def call(ctx, name, fn, *args):
        replaying = ctx.is_replaying
        out = ctx.step(name, fn, *args)
        shown = out if isinstance(out, str) else f"{len(out)} results"
        print(f"  [{'replayed' if replaying else 'executed'}] {name:<22} {str(shown)[:60]}")
        return out

    @engine.workflow()
    def research_agent(ctx, question: str):
        plan = call(ctx, "llm:plan", fake_llm, f"plan {question}", 1200)
        sources = []
        for i, q in enumerate(s.split(": ", 1)[1] for s in plan.split("; ")):
            sources += call(ctx, f"tool:search[{i}]", web_search, q)
        draft = call(ctx, "llm:draft", fake_llm, f"draft {question} {sources}", 6000)
        return call(ctx, "llm:final", fake_llm, f"final {draft}", 3000)

    return engine


def main() -> None:
    sys.stdout.reconfigure(line_buffering=True)  # os._exit skips flushing buffered output
    p = argparse.ArgumentParser()
    p.add_argument("--crash-after", type=int, help="kill the process after N journaled steps")
    p.add_argument("--fresh", action="store_true", help="delete the journal and start over")
    args = p.parse_args()
    if args.fresh:
        for suffix in ("", "-wal", "-shm"):
            Path(str(DB) + suffix).unlink(missing_ok=True)

    engine = build(args.crash_after)
    run_id = engine.start("research_agent", "How should we index 100M vectors with ACL filters?",
                          run_id="demo")  # fixed id: re-running the script resumes this run
    status = engine.get_run(run_id)["status"]
    if status == "completed":
        print("Run already completed (use --fresh to start over):", engine.result(run_id))
        return
    print(f"run {run_id!r} ({status}); worker lease expires 1s after a crash, waiting...\n")
    t0 = time.monotonic()
    run = engine.wait(run_id, timeout=60)
    print(f"\nstatus: {run['status']}  result: {run['result']!r}")
    print(f"this process: {spent['llm_calls']} LLM calls, ${spent['usd']:.4f}, {time.monotonic() - t0:.1f}s")


if __name__ == "__main__":
    main()
