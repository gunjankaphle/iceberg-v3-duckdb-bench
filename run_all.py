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
# trigger errors that Spark 4 logs as multi-KB JSON records on stderr. Drop
# those log lines so the report card stays readable.
#
# ERROR-level lines are deliberately NOT filtered: an earlier version matched
# `(WARN|INFO|ERROR)\s` and swallowed genuine failures like
# "ERROR SparkContext: Failed to initialize". The expected probe errors arrive
# as {"ts":...} JSON records, which the first alternative already covers.
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
    if a.rows < 1:
        ap.error("--rows must be a positive integer")

    if not a.verify_only:
        if run("build.py", "--rows", str(a.rows)) != 0:
            print("build failed", file=sys.stderr)
            return 1
    rc = run("verify.py")

    # A broken documented command is a release blocker, too.
    docs = subprocess.run([sys.executable, os.path.join(HERE, "tools", "check_docs.py")],
                          cwd=HERE, capture_output=True, text=True)
    if docs.returncode != 0:
        print("\nDOC CHECK\n" + docs.stdout.strip(), flush=True)
        return docs.returncode
    return rc


if __name__ == "__main__":
    sys.exit(main())
