@echo off
setlocal

REM Wrapper to start BenchVue CSV ingestion on Windows
REM Uses a specific Python interpreter and default directories

set "PY=C:\Users\qris\winPython\WPy64-31241\python-3.12.4.amd64\python.exe"
set "DIR=C:\Users\qris\Documents\LEMS\Keysight logs"
set "DB=C:\Users\qris\py_automations\data_log\benchvue.sqlite3"

"%PY%" "%~dp0hermaeus-mora.py" --dir "%DIR%" --db "%DB%" %*

endlocal
