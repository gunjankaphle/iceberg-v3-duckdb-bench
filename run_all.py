#!/usr/bin/env python3
"""Build the Iceberg v3 tables with Spark, then verify them with DuckDB.

    python run_all.py                # full run, 5M-row perf tables
    python run_all.py --rows 500000  # faster run
    python run_all.py --verify-only  # re-run DuckDB checks against existing tables
"""
import argparse
import os
import re
import subprocess
import sys
from collections import deque

HERE = os.path.dirname(os.path.abspath(__file__))
BENCH = os.path.join(HERE, "bench")

# Spark is chatty, and the writer-capability probes in build.py deliberately
# trigger errors that Spark 4 logs as multi-KB JSON records on stderr. Drop
# those log lines so the report card stays readable.
#
# Preserve the complete output in a log; show the tail if a subprocess fails.
NOISE = re.compile(
    r'^\s*(\{"ts":'                      # Spark 4 structured JSON log records
    r'|\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}'  # classic log4j lines
    r'|(WARN|INFO)\s'
    r'|:: |\[Stage |Ivy Default Cache|The jars for the packages'
    r'|.*\badded as a dependency\b'
    r'|\s*(confs:|found |downloading |\[SUCCESSFUL\]|:: resolution report))'
)


def run(script, *args):
    print(f"\n{'=' * 72}\n{script}\n{'=' * 72}", flush=True)
    p = subprocess.Popen(
        [sys.executable, "-u", os.path.join(BENCH, script), *args],
        cwd=BENCH, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, errors="replace", bufsize=1,
    )
    blank = False
    log_dir = os.path.join(HERE, "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, script + ".log")
    tail = deque(maxlen=40)
    with open(log_path, "w", encoding="utf-8") as log:
        for line in p.stdout:
            log.write(line)
            log.flush()
            tail.append(line)
            visible = line.rstrip("\n").split("\r")[-1]
            if NOISE.match(visible) and "ERROR" not in visible and '"level":"ERROR"' not in visible:
                continue
            if not visible.strip():
                blank = True
                continue
            if blank:
                print(flush=True)
                blank = False
            # Expected probe stack traces remain in the log, not the report.
            if visible.startswith('{"ts":'):
                continue
            print(visible, flush=True)
    rc = p.wait()
    if rc:
        print(f"\n{script} failed; last output (full log: {log_path}):", file=sys.stderr)
        print("".join(tail), file=sys.stderr)
    return rc


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=5_000_000)
    ap.add_argument("--verify-only", action="store_true")
    ap.add_argument("--results", help="write sanitized results to this JSON path")
    a = ap.parse_args()
    if a.rows < 1000:
        ap.error("--rows must be at least 1000 for the delete-artifact workload")

    for command in ([sys.executable, os.path.join(HERE, "tools", "check_docs.py")],
                    [sys.executable, "-m", "unittest", "discover", "-s", "tests"]):
        check = subprocess.run(command, cwd=HERE)
        if check.returncode:
            return check.returncode

    if not a.verify_only:
        if run("build.py", "--rows", str(a.rows)) != 0:
            print("build failed", file=sys.stderr)
            return 1
    return run("verify.py", *(["--results", os.path.abspath(a.results)] if a.results else []))


if __name__ == "__main__":
    sys.exit(main())
