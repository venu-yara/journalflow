"""SQLite-backed journal: runs, their event histories, and worker leases.

Every write that belongs to a run is *fenced*: it only commits if the writer still
holds the run's current lease epoch. A worker that stalled (GC pause, network
partition, laptop lid closed) and lost its lease to another worker cannot corrupt
the journal when it wakes up -- its writes are rejected with :class:`LeaseLost`.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id        TEXT PRIMARY KEY,
    workflow      TEXT NOT NULL,
    input         TEXT NOT NULL,
    status        TEXT NOT NULL,   -- pending|running|sleeping|completed|failed|dead
    result        TEXT,
    error         TEXT,
    wake_at       REAL,
    lease_owner   TEXT,
    lease_epoch   INTEGER NOT NULL DEFAULT 0,
    lease_expires REAL,
    crash_claims  INTEGER NOT NULL DEFAULT 0,
    created_at    REAL NOT NULL,
    updated_at    REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS runs_ready ON runs (status, wake_at);

CREATE TABLE IF NOT EXISTS events (
    run_id     TEXT NOT NULL REFERENCES runs (run_id),
    seq        INTEGER NOT NULL,
    kind       TEXT NOT NULL,      -- step|step_failed|timer
    name       TEXT NOT NULL,
    payload    TEXT NOT NULL,
    epoch      INTEGER NOT NULL,   -- lease epoch that wrote it (audit trail)
    created_at REAL NOT NULL,
    PRIMARY KEY (run_id, seq)      -- defense in depth: one outcome per step, ever
);
"""

ACTIVE = ("pending", "running", "sleeping")
TERMINAL = ("completed", "failed", "dead")


class LeaseLost(BaseException):
    """This worker no longer owns the run. Derives from BaseException so that
    ``except Exception`` in user workflow code cannot swallow it."""


@dataclass(frozen=True)
class Lease:
    run_id: str
    workflow: str
    input: list
    owner: str
    epoch: int


@dataclass(frozen=True)
class Event:
    seq: int
    kind: str
    name: str
    payload: dict
    epoch: int


class Store:
    def __init__(self, path: str) -> None:
        # isolation_level=None: we issue BEGIN IMMEDIATE ourselves, which takes the
        # write lock up front and makes claim/append races impossible.
        self.conn = sqlite3.connect(path, timeout=30, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        # NORMAL in WAL mode survives process crashes; an OS crash may roll back the
        # last few commits, which is safe because steps are at-least-once anyway.
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA busy_timeout=30000")
        self.conn.executescript(SCHEMA)

    def close(self) -> None:
        self.conn.close()

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        self.conn.execute("COMMIT")

    # ------------------------------------------------------------- run lifecycle

    def create_run(self, run_id: str, workflow: str, args: list, now: float) -> bool:
        """Insert a run. Returns False if ``run_id`` already exists (idempotent start)."""
        with self.tx() as c:
            cur = c.execute(
                "INSERT OR IGNORE INTO runs (run_id, workflow, input, status, created_at, updated_at)"
                " VALUES (?, ?, ?, 'pending', ?, ?)",
                (run_id, workflow, json.dumps(args), now, now),
            )
            return cur.rowcount == 1

    def claim(
        self, workflows: list[str], owner: str, now: float, ttl: float, max_crash_claims: int
    ) -> Lease | None:
        """Atomically take the lease on one ready run, or return None.

        A run found in status ``running`` with an expired lease means its previous
        worker died without releasing it. After ``max_crash_claims`` such deaths with
        no journal progress in between, the run is a poison pill and is moved to
        ``dead`` instead of crash-looping the fleet forever.
        """
        if not workflows:
            return None
        marks = ",".join("?" * len(workflows))
        with self.tx() as c:
            while True:
                row = c.execute(
                    f"SELECT run_id, workflow, input, status, lease_epoch, crash_claims FROM runs"
                    f" WHERE status IN ('pending', 'running', 'sleeping') AND workflow IN ({marks})"
                    f"   AND (lease_expires IS NULL OR lease_expires <= ?)"
                    f"   AND (wake_at IS NULL OR wake_at <= ?)"
                    f" ORDER BY COALESCE(wake_at, created_at) LIMIT 1",
                    (*workflows, now, now),
                ).fetchone()
                if row is None:
                    return None
                crashes = row["crash_claims"] + (row["status"] == "running")
                if crashes >= max_crash_claims:
                    c.execute(
                        "UPDATE runs SET status='dead', crash_claims=?, lease_owner=NULL,"
                        " lease_expires=NULL, updated_at=?,"
                        " error='worker crashed ' || ? || ' times while running this workflow' WHERE run_id=?",
                        (crashes, now, crashes, row["run_id"]),
                    )
                    continue
                epoch = row["lease_epoch"] + 1
                c.execute(
                    "UPDATE runs SET status='running', lease_owner=?, lease_epoch=?, lease_expires=?,"
                    " crash_claims=?, wake_at=NULL, updated_at=? WHERE run_id=?",
                    (owner, epoch, now + ttl, crashes, now, row["run_id"]),
                )
                return Lease(row["run_id"], row["workflow"], json.loads(row["input"]), owner, epoch)

    def _fence(
        self, c: sqlite3.Connection, lease: Lease, now: float, ttl: float, progress: bool = False
    ) -> None:
        # Journal progress resets the crash counter: a poison pill crashes at the
        # same step every time, while a run that merely had bad luck moves forward.
        # (Found by the chaos test: counting raw crashes dead-lettered healthy runs.)
        cur = c.execute(
            "UPDATE runs SET lease_expires=?, updated_at=?"
            + (", crash_claims=0" if progress else "")
            + " WHERE run_id=? AND lease_owner=? AND lease_epoch=? AND status='running'",
            (now + ttl, now, lease.run_id, lease.owner, lease.epoch),
        )
        if cur.rowcount != 1:
            raise LeaseLost(f"{lease.owner} lost lease epoch {lease.epoch} on run {lease.run_id}")

    def renew(self, lease: Lease, now: float, ttl: float) -> None:
        with self.tx() as c:
            self._fence(c, lease, now, ttl)

    def append(
        self, lease: Lease, seq: int, kind: str, name: str, payload: dict, now: float, ttl: float
    ) -> None:
        """Fenced append of one journal event (also renews the lease)."""
        with self.tx() as c:
            self._fence(c, lease, now, ttl, progress=True)
            c.execute(
                "INSERT INTO events (run_id, seq, kind, name, payload, epoch, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (lease.run_id, seq, kind, name, json.dumps(payload), lease.epoch, now),
            )

    def release(
        self,
        lease: Lease,
        status: str,
        now: float,
        *,
        result: Any = None,
        error: str | None = None,
        wake_at: float | None = None,
    ) -> None:
        """Fenced hand-back of a run: finished, failed, or sleeping until ``wake_at``."""
        with self.tx() as c:
            cur = c.execute(
                "UPDATE runs SET status=?, result=?, error=?, wake_at=?, lease_owner=NULL,"
                " lease_expires=NULL, crash_claims=0, updated_at=?"
                " WHERE run_id=? AND lease_owner=? AND lease_epoch=? AND status='running'",
                (status, None if result is None else json.dumps(result), error, wake_at, now,
                 lease.run_id, lease.owner, lease.epoch),
            )
            if cur.rowcount != 1:
                raise LeaseLost(f"{lease.owner} lost lease epoch {lease.epoch} on run {lease.run_id}")

    # ------------------------------------------------------------------- queries

    def events(self, run_id: str) -> list[Event]:
        rows = self.conn.execute(
            "SELECT seq, kind, name, payload, epoch FROM events WHERE run_id=? ORDER BY seq", (run_id,)
        ).fetchall()
        return [Event(r["seq"], r["kind"], r["name"], json.loads(r["payload"]), r["epoch"]) for r in rows]

    def get_run(self, run_id: str) -> dict | None:
        row = self.conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            return None
        run = dict(row)
        run["input"] = json.loads(run["input"])
        run["result"] = None if run["result"] is None else json.loads(run["result"])
        return run

    def list_runs(self, status: str | None = None) -> list[dict]:
        sql = "SELECT run_id, workflow, status, error, updated_at FROM runs"
        rows = self.conn.execute(sql + (" WHERE status=?" if status else "") + " ORDER BY created_at",
                                 (status,) if status else ()).fetchall()
        return [dict(r) for r in rows]

    def reset_for_retry(self, run_id: str, now: float) -> None:
        """Move a failed/dead run back to pending. If the journal ends in a recorded
        step failure, drop that event so the step is attempted again on replay."""
        with self.tx() as c:
            row = c.execute("SELECT status FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None:
                raise KeyError(run_id)
            if row["status"] not in ("failed", "dead"):
                raise ValueError(f"run {run_id} is {row['status']}; only failed/dead runs can be retried")
            last = c.execute(
                "SELECT seq, kind FROM events WHERE run_id=? ORDER BY seq DESC LIMIT 1", (run_id,)
            ).fetchone()
            if last is not None and last["kind"] == "step_failed":
                c.execute("DELETE FROM events WHERE run_id=? AND seq=?", (run_id, last["seq"]))
            c.execute(
                "UPDATE runs SET status='pending', error=NULL, result=NULL, crash_claims=0,"
                " wake_at=NULL, lease_owner=NULL, lease_expires=NULL, updated_at=? WHERE run_id=?",
                (now, run_id),
            )
