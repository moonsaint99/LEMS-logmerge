This repo provides a script that'll watch a folder containing CSV files recorded by Keysight BenchVue and assimilate those values into a SQLite database.

BenchVue sometimes crashes and restarts, creating a new CSV with a new timestamp in its filename. The watcher handles both cases:
- New lines appended to existing CSVs
- New CSV files appearing (e.g., when BenchVue restarts)

Two auto-exports typically run simultaneously (e.g., `iso` and `40`). Both are handled in tandem.

What gets written
- Table: `samples`
- Columns:
  - `timestamp` (TEXT): The timestamp in the first column of each data row
  - `source` (TEXT): Derived from filename after `AutoExportTrace_` (e.g., `iso`, `40`)
  - `channel` (TEXT): The column header (e.g., `116 (Vdc)- EGSE7V`)
  - `value` (REAL): The numeric value for that channel at the timestamp
  - `extra` (TEXT): The CSV filename that the value came from

CSV format assumptions
- The CSV contains a variable-length preamble.
- The header row begins with: `Scan Sweep Time (Sec),Scan Number, ...`
- Data rows follow: `timestamp,scan_number,<values...>`
- Header names (after `Scan Number`) are used as `channel`.

Usage
- Install Python 3.9+.
- Run the ingester:

  `python hermaeus-mora.py --dir <path-to-folder> --db <path-to-sqlite>`

  Options:
  - `--interval <seconds>`: Polling interval (default: 1.0)
  - `--backfill`: Ingest existing lines already in files at startup (default: off)

Environment variables
- `BENCHVUE_DIR`: Default directory to watch (overridden by `--dir`)
- `BENCHVUE_DB`: Default SQLite DB path (overridden by `--db`)

Quick check with the included examples
- Print a few parsed measurements from `example_csvs/`:

  `python watcher.py example_csvs --backfill | head`

Notes
- The watcher starts at end-of-file by default to avoid duplicate inserts when restarting. Use `--backfill` if you want to ingest historical data present at startup.
- The script ignores non-numeric/blank values.

Windows defaults and quick-start
- Default watch dir: `C:\Users\qris\Documents\LEMS\Keysight logs`
- Default DB path: `C:\Users\qris\py_automations\data_log\benchvue.sqlite3`
- Wrapper: double-click or run `start-logger.cmd` to launch using `C:\Users\qris\winPython\WPy64-31241\python-3.12.4.amd64\python.exe`.
- You can still pass extra arguments and they will be forwarded, e.g. `start-logger.cmd --backfill`.
