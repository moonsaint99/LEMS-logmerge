"""
Hermaeus Mora — DB ingester
---------------------------

Consumes the `watcher.watch` stream and writes each measurement into an
SQLite database table `samples(timestamp TEXT, source TEXT, channel TEXT,
value REAL, extra TEXT)`.

Usage:
    python hermaeus-mora.py --dir <watch_dir> --db <db_path> [--backfill]

Defaults:
    - DB path via env `BENCHVUE_DB` or falls back to a local `benchvue.sqlite3`.
    - Directory via env `BENCHVUE_DIR` or current working directory.
"""

from __future__ import annotations

import argparse
import os
import signal
import sqlite3
import sys
import time
from typing import Optional
from datetime import datetime
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:  # pragma: no cover - fallback if unavailable
    ZoneInfo = None  # type: ignore

import watcher


# Default database path (override with BENCHVUE_DB env var if desired)
DEFAULT_DB_PATH = os.environ.get(
    "BENCHVUE_DB", r"C:\\Users\\qris\\py_automations\\data_log\\benchvue.sqlite3"
)


STOP = False


def _handle_sigint(signum, frame):
    global STOP
    STOP = True
    print("Stopping after current batch...")


def _connect_db(db_path: str) -> sqlite3.Connection:
    # Ensure parent directory exists
    parent = os.path.dirname(db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS samples (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            source TEXT NOT NULL,
            channel TEXT NOT NULL,
            value REAL,
            extra TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_samples_timestamp ON samples(timestamp)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_samples_source ON samples(source)")
    conn.commit()
    return conn


def _has_unique_dedup_index(db: sqlite3.Connection) -> bool:
    try:
        rows = db.execute("PRAGMA index_list(samples)").fetchall()
    except sqlite3.DatabaseError:
        return False
    for row in rows:
        # row schema: seq, name, unique, origin, partial
        name = row[1]
        unique = row[2]
        if unique != 1:
            continue
        try:
            info = db.execute(f"PRAGMA index_info({name})").fetchall()
        except sqlite3.DatabaseError:
            continue
        cols = [r[2] for r in info]
        if cols == ["timestamp", "source", "channel"]:
            return True
    return False


def _ensure_unique_index(db: sqlite3.Connection) -> bool:
    if _has_unique_dedup_index(db):
        return True
    try:
        db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uniq_samples_key ON samples(timestamp, source, channel)"
        )
        db.commit()
    except sqlite3.DatabaseError:
        try:
            db.rollback()
        except Exception:
            pass
    return _has_unique_dedup_index(db)


def _insert_sample(
    db: sqlite3.Connection,
    ts: str,
    source: str,
    channel: str,
    value,
    extra: Optional[str] = None,
    use_or_ignore: bool = False,
):
    if use_or_ignore:
        db.execute(
            "INSERT OR IGNORE INTO samples (timestamp, source, channel, value, extra) VALUES (?, ?, ?, ?, ?)",
            (ts, source, channel, value, extra),
        )
    else:
        db.execute(
            (
                "INSERT INTO samples (timestamp, source, channel, value, extra)\n"
                "SELECT ?, ?, ?, ?, ?\n"
                "WHERE NOT EXISTS (\n"
                "  SELECT 1 FROM samples WHERE timestamp = ? AND source = ? AND channel = ?\n"
                ")"
            ),
            (ts, source, channel, value, extra, ts, source, channel),
        )


def _tune_for_backfill(db: sqlite3.Connection, aggressive: bool) -> None:
    try:
        if aggressive:
            # Fastest but reduces durability during the run
            db.execute("PRAGMA journal_mode=MEMORY")
            db.execute("PRAGMA synchronous=OFF")
        else:
            # Safer defaults that are still faster for bulk loads
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=NORMAL")
        db.execute("PRAGMA temp_store=MEMORY")
    except sqlite3.DatabaseError:
        pass


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest BenchVue CSV updates into SQLite")
    parser.add_argument(
        "--dir",
        dest="directory",
        default=os.environ.get("BENCHVUE_DIR", r"C:\\Users\\qris\\Documents\\LEMS\\Keysight logs"),
        help="Directory to watch for AutoExportTrace_*.csv",
    )
    parser.add_argument(
        "--db",
        dest="db_path",
        default=DEFAULT_DB_PATH,
        help="SQLite database file path",
    )
    parser.add_argument(
        "--interval",
        dest="interval",
        type=float,
        default=1.0,
        help="Polling interval in seconds",
    )
    parser.add_argument(
        "--no-progress",
        dest="no_progress",
        action="store_true",
        help="Disable per-insert progress indicator output",
    )
    parser.add_argument(
        "--fast-backfill",
        action="store_true",
        help="Speed up backfill by relaxing SQLite durability settings",
    )
    parser.add_argument(
        "--commit-rows",
        type=int,
        default=None,
        help="Rows per commit (overrides auto-tuned defaults)",
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Process existing lines already present in files at startup",
    )
    args = parser.parse_args(argv)

    print(f"Watching: {args.directory}")
    print(f"Database: {args.db_path}")
    if args.backfill:
        print("Backfill: enabled (processing existing file contents)")

    signal.signal(signal.SIGINT, _handle_sigint)
    signal.signal(signal.SIGTERM, _handle_sigint)

    db = _connect_db(args.db_path)
    # Attempt to add a unique index to let us use INSERT OR IGNORE
    has_unique = _ensure_unique_index(db)

    # If we're doing backfill, tune SQLite for faster bulk loads
    if args.backfill:
        _tune_for_backfill(db, aggressive=args.fast_backfill)

    attempts = 0
    base_changes = 0
    try:
        base_changes = db.total_changes
    except Exception:
        base_changes = 0
    last_commit = time.time()
    # Auto-tune default batching; allow override via --commit-rows
    commit_every = 250  # rows
    commit_seconds = 2.0  # seconds
    if args.backfill and args.commit_rows is None:
        commit_every = 5000 if args.fast_backfill else 1500
        commit_seconds = 5.0 if args.fast_backfill else 3.0
    if args.commit_rows is not None and args.commit_rows > 0:
        commit_every = args.commit_rows

    try:
        show_progress = not args.no_progress
        az_tz = ZoneInfo("America/Phoenix") if ZoneInfo else None
        for ts, source, channel, value, extra in watcher.watch(
            args.directory,
            poll_interval=args.interval,
            backfill=args.backfill,
            should_stop=lambda: STOP,
        ):
            # Track whether an insert actually occurred (dedupe may skip it)
            before = db.total_changes
            _insert_sample(db, ts, source, channel, value, extra, use_or_ignore=has_unique)
            after = db.total_changes
            attempts += 1

            if show_progress and after > before:
                # Print a lightweight indicator with Arizona (America/Phoenix) time
                now_dt = datetime.now(az_tz) if az_tz is not None else datetime.now()
                now_str = now_dt.strftime("%Y-%m-%d %H:%M:%S %Z")
                print(f"[{now_str}] +1 row -> {source} | {channel}", flush=True)

            now = time.time()
            if attempts % commit_every == 0 or (now - last_commit) >= commit_seconds:
                db.commit()
                last_commit = now

            if STOP:
                break
    finally:
        db.commit()
        changes = 0
        try:
            changes = db.total_changes - base_changes
        except Exception:
            pass
        db.close()
        print(f"Inserted rows: {changes}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
