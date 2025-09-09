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

import watcher


# Default database path (override with BENCHVUE_DB env var if desired)
DEFAULT_DB_PATH = os.environ.get(
    "BENCHVUE_DB", os.path.join(os.getcwd(), "benchvue.sqlite3")
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


def _insert_sample(
    db: sqlite3.Connection,
    ts: str,
    source: str,
    channel: str,
    value,
    extra: Optional[str] = None,
):
    db.execute(
        "INSERT INTO samples (timestamp, source, channel, value, extra) VALUES (?, ?, ?, ?, ?)",
        (ts, source, channel, value, extra),
    )


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest BenchVue CSV updates into SQLite")
    parser.add_argument(
        "--dir",
        dest="directory",
        default=os.environ.get("BENCHVUE_DIR", os.getcwd()),
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

    inserted = 0
    last_commit = time.time()
    commit_every = 250  # rows
    commit_seconds = 2.0  # seconds

    try:
        for ts, source, channel, value, extra in watcher.watch(
            args.directory,
            poll_interval=args.interval,
            backfill=args.backfill,
            should_stop=lambda: STOP,
        ):
            _insert_sample(db, ts, source, channel, value, extra)
            inserted += 1

            now = time.time()
            if inserted % commit_every == 0 or (now - last_commit) >= commit_seconds:
                db.commit()
                last_commit = now

            if STOP:
                break
    finally:
        db.commit()
        db.close()
        print(f"Inserted rows: {inserted}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
