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

HERE = os.path.dirname(os.path.abspath(__file__))
BENCH = os.path.join(HERE, "bench")

# Spark is chatty, and the writer-capability probes in build.py deliberately
# trigger errors that Spark 4 logs as multi-KB JSON records on stderr. Drop the
# log lines so the report card stays readable; real output is unaffected.
NOISE = re.compile(
    r'^\s*(\{"ts":'                      # Spark 4 structured JSON log records
    r'|\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}'  # classic log4j lines
    r'|(WARN|INFO|ERROR)\s'
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
    for line in p.stdout:
        # Spark's progress bar uses \r; keep only what follows the last one.
        visible = line.rstrip("\n").split("\r")[-1]
        if NOISE.match(visible):
            continue
        if not visible.strip():
            blank = True  # collapse runs of blank lines rather than dropping them
            continue
        if blank:
            print(flush=True)
            blank = False
        print(visible, flush=True)
    return p.wait()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=5_000_000)
    ap.add_argument("--verify-only", action="store_true")
    a = ap.parse_args()

    if not a.verify_only:
        if run("build.py", "--rows", str(a.rows)) != 0:
            print("build failed", file=sys.stderr)
            return 1
    return run("verify.py")


if __name__ == "__main__":
    sys.exit(main())
