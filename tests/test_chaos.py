"""Multi-process chaos test (real processes, real os._exit). Takes a few seconds."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "chaos"))

import run_chaos  # noqa: E402


def test_chaos_all_runs_complete_with_exactly_once_dedup(tmp_path):
    report = run_chaos.run(runs=12, steps=4, workers=3, crash_prob=0.06, flaky=0.1,
                           lease_ttl=0.5, timeout=120, workdir=str(tmp_path))
    assert report["failures"] == []
    assert report["worker_crashes_injected"] > 0, "chaos should actually have killed workers"
    assert report["poison_status"] == "dead"
    assert report["side_effects_deduplicated"] == 12 * 4
