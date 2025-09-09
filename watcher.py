# This script watches a directory for BenchVue CSV exports (e.g. AutoExportTrace_iso ...
# and AutoExportTrace_40 ...) and streams newly appended data rows.
"""
Watcher module
---------------

Exposes a `watch` generator which monitors a directory for CSV files that
BenchVue appends to. It detects the header row ("Scan Sweep Time (Sec),Scan Number,...")
and then yields measurements for any new data rows appended to each file.

Yielded measurement tuple:
    (timestamp_str, source, channel, value_float, extra_filename)

Where:
    - source: parsed from filename after `AutoExportTrace_` (e.g. "iso" or "40")
    - channel: the column header as found in the CSV (e.g. "116 (Vdc)- EGSE7V")
    - value: float parsed from the data row; rows with non-numeric/blank values are skipped
    - extra: the filename the value came from

Notes:
    - Supports multiple files at once (e.g., iso and 40 running simultaneously).
    - Handles new files appearing and existing files growing.
    - By default starts tailing at end-of-file to avoid backfilling old data; this
      can be changed with the `backfill=True` argument.
"""

from __future__ import annotations

import csv
import glob
import io
import os
import re
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, Generator, Iterable, List, Optional, Tuple


HEADER_LEADER = "Scan Sweep Time (Sec)"
FILENAME_SOURCE_RE = re.compile(r"AutoExportTrace_([^\s]+)\s", re.IGNORECASE)
CSV_GLOB = "AutoExportTrace_*.csv"


def _parse_source_from_filename(path: str) -> str:
    base = os.path.basename(path)
    m = FILENAME_SOURCE_RE.search(base)
    if m:
        return m.group(1)
    # fallback: use everything before first space after underscore
    try:
        after = base.split("AutoExportTrace_", 1)[1]
        return after.split(" ", 1)[0]
    except Exception:
        return base


@dataclass
class FileState:
    path: str
    source: str
    backfill: bool = False
    header_found: bool = False
    channels: List[str] = field(default_factory=list)
    channel_cols: List[int] = field(default_factory=list)
    pos: int = 0  # byte offset within file
    remainder: str = ""  # partial last line buffer
    encoding: str = "utf-8-sig"

    def ensure_header_and_position(self) -> None:
        # Always scan from beginning once to capture header position and channels
        try:
            with open(self.path, "rb") as f:
                self.pos = 0
                self.header_found = False
                while True:
                    raw = f.readline()
                    if not raw:
                        break
                    self.pos = f.tell()
                    try:
                        line = raw.decode(self.encoding, errors="ignore")
                    except Exception:
                        continue
                    row = next(csv.reader([line]))
                    # Detect header row robustly: second column literally says Scan Number
                    if row and len(row) > 1 and row[1].strip().lower() == "scan number":
                        # Capture only non-empty channel names and their absolute column indexes
                        ch_names: List[str] = []
                        ch_cols: List[int] = []
                        for idx in range(2, len(row)):
                            name = row[idx].strip()
                            if name == "":
                                continue
                            ch_names.append(name)
                            ch_cols.append(idx)
                        self.channels = ch_names
                        self.channel_cols = ch_cols
                        self.header_found = True
                        break
                # Position after header line or end
                if not self.header_found:
                    # No header yet; position at current end
                    self.channels = []
                    self.pos = f.tell()
        except (FileNotFoundError, PermissionError):
            # File might race-disappear; ignore
            pass

        # If not backfilling, set position to end-of-file so we only watch new lines
        if not self.backfill:
            try:
                self.pos = os.path.getsize(self.path)
            except (FileNotFoundError, PermissionError):
                self.pos = 0

    def read_new_measurements(self) -> Iterable[Tuple[str, str, str, float, str]]:
        """Read newly appended content from the file and yield measurements.

        Yields tuples: (timestamp, source, channel, value, extra)
        """
        extra = os.path.basename(self.path)
        try:
            size = os.path.getsize(self.path)
        except (FileNotFoundError, PermissionError):
            return []

        # If truncated, reset position and remainder
        if size < self.pos:
            self.pos = 0
            self.remainder = ""
            self.header_found = False
            self.channels = []

        produced: List[Tuple[str, str, str, float, str]] = []

        try:
            with open(self.path, "rb") as f:
                # If header not known, try to discover from current pos forward
                if not self.header_found:
                    f.seek(self.pos)
                    while True:
                        raw = f.readline()
                        if not raw:
                            break
                        self.pos = f.tell()
                        try:
                            line = raw.decode(self.encoding, errors="ignore")
                        except Exception:
                            continue
                        row = next(csv.reader([line]))
                        if row and len(row) > 1 and row[1].strip().lower() == "scan number":
                            ch_names: List[str] = []
                            ch_cols: List[int] = []
                            for idx in range(2, len(row)):
                                name = row[idx].strip()
                                if name == "":
                                    continue
                                ch_names.append(name)
                                ch_cols.append(idx)
                            self.channels = ch_names
                            self.channel_cols = ch_cols
                            self.header_found = True
                            break

                # After header, stream data rows
                f.seek(self.pos)
                raw_chunk = f.read()
                if not raw_chunk:
                    return []
                try:
                    chunk = raw_chunk.decode(self.encoding, errors="ignore")
                except Exception:
                    chunk = ""
                data = self.remainder + chunk
                lines = data.splitlines(keepends=True)
                # Keep last line if not newline-terminated
                if lines and not (lines[-1].endswith("\n") or lines[-1].endswith("\r")):
                    self.remainder = lines[-1]
                    lines = lines[:-1]
                else:
                    self.remainder = ""

                reader = csv.reader([ln.rstrip("\r\n") for ln in lines])
                for row in reader:
                    # Skip until header appears
                    if not self.header_found:
                        if row and len(row) > 1 and row[1].strip().lower() == "scan number":
                            ch_names: List[str] = []
                            ch_cols: List[int] = []
                            for idx in range(2, len(row)):
                                name = row[idx].strip()
                                if name == "":
                                    continue
                                ch_names.append(name)
                                ch_cols.append(idx)
                            self.channels = ch_names
                            self.channel_cols = ch_cols
                            self.header_found = True
                        continue

                    if not row or len(row) < 2:
                        continue
                    ts = row[0].strip()
                    # Use presence of a numeric scan number to accept the row
                    scan_str = row[1].strip() if len(row) > 1 else ""
                    try:
                        int(scan_str)
                    except Exception:
                        continue
                    # Emit values by absolute column positions bound to named channels
                    if not self.channels or not self.channel_cols:
                        continue
                    for name, col in zip(self.channels, self.channel_cols):
                        if col >= len(row):
                            continue
                        v = row[col].strip()
                        if v == "":
                            continue
                        try:
                            val = float(v)
                        except ValueError:
                            continue
                        produced.append((ts, self.source, name, val, extra))

                # Advance position by number of bytes we consumed
                self.pos += len(raw_chunk)
        except (FileNotFoundError, PermissionError):
            # File vanished mid-read
            return []

        return produced


def watch(
    directory: str,
    *,
    poll_interval: float = 1.0,
    backfill: bool = False,
    should_stop: Optional[Callable[[], bool]] = None,
) -> Generator[Tuple[str, str, str, float, str], None, None]:
    """Watch `directory` for AutoExportTrace CSV files and yield measurements.

    Args:
        directory: Folder to monitor
        poll_interval: Seconds between polls
        backfill: If True, process historical lines already in files at startup
        should_stop: Optional callable that returns True to stop watching
    """
    tracked: Dict[str, FileState] = {}

    # Initial discovery
    for path in glob.glob(os.path.join(directory, CSV_GLOB)):
        src = _parse_source_from_filename(path)
        st = FileState(path=path, source=src, backfill=backfill)
        st.ensure_header_and_position()
        tracked[path] = st

    while True:
        if should_stop and should_stop():
            return

        # Discover new files
        seen = set()
        for path in glob.glob(os.path.join(directory, CSV_GLOB)):
            seen.add(path)
            if path not in tracked:
                src = _parse_source_from_filename(path)
                st = FileState(path=path, source=src, backfill=backfill)
                st.ensure_header_and_position()
                tracked[path] = st

        # Drop files that disappeared
        for path in list(tracked.keys()):
            if path not in seen and not os.path.exists(path):
                tracked.pop(path, None)

        # Pull new data from each file
        for st in tracked.values():
            for meas in st.read_new_measurements():
                yield meas

        time.sleep(poll_interval)


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Watch BenchVue CSV directory and print new measurements")
    ap.add_argument("directory", nargs="?", default=os.environ.get("BENCHVUE_DIR", r"C:\\Users\\qris\\Documents\\LEMS\\Keysight logs"))
    ap.add_argument("--backfill", action="store_true", help="Process existing file contents at startup")
    ap.add_argument("--interval", type=float, default=7.4, help="Polling interval in seconds")
    args = ap.parse_args()

    try:
        for ts, src, ch, val, extra in watch(args.directory, poll_interval=args.interval, backfill=args.backfill):
            print(f"{ts}\t{src}\t{ch}\t{val}\t{extra}")
    except KeyboardInterrupt:
        pass
