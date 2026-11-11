"""A worker process that kills itself (``os._exit``, no cleanup, no finally blocks)
with probability ``crash_prob`` at each fault point."""

from __future__ import annotations

import os
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import app  # noqa: E402

CRASH_EXIT = 137


def main() -> None:
    db, crash_prob, lease_ttl = sys.argv[1], float(sys.argv[2]), float(sys.argv[3])
    rng = random.Random()

    def chaos(point, info):
        if rng.random() < crash_prob:
            os._exit(CRASH_EXIT)

    engine = app.build(db, faults=chaos, lease_ttl=lease_ttl)
    engine.run_forever(poll_interval=0.02)


if __name__ == "__main__":
    main()
