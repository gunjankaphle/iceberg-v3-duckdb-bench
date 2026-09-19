#!/usr/bin/env python3
"""Check that fenced code blocks in the docs are at least syntactically valid.

A ```python block that doesn't parse is a doc bug that looks fine in review and
fails the moment someone pastes it. This catches that class cheaply.

    python3 tools/check_docs.py          # exits non-zero on any bad block
"""
import ast
import glob
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FENCE = re.compile(r"```(\w+)\n(.*?)```", re.S)


def check(path):
    problems = []
    text = open(path).read()
    for n, (lang, body) in enumerate(FENCE.findall(text), 1):
        if lang == "python":
            try:
                ast.parse(body)
            except SyntaxError as e:
                line = body.splitlines()[max(0, e.lineno - 1)] if body.splitlines() else ""
                problems.append(f"{path} python block {n}: {e.msg} (line {e.lineno}): {line.strip()[:60]}")
        elif lang in ("bash", "sh"):
            r = subprocess.run(["bash", "-n"], input=body, text=True,
                               capture_output=True)
            if r.returncode != 0:
                problems.append(f"{path} {lang} block {n}: {r.stderr.strip().splitlines()[0][:80]}")
    return problems


def main():
    files = sorted(glob.glob(os.path.join(ROOT, "*.md")))
    problems = [p for f in files for p in check(f)]
    for p in problems:
        print(f"  ✗ {p}")
    checked = ", ".join(os.path.basename(f) for f in files)
    print(f"{'FAIL' if problems else 'OK'}: {len(problems)} problem(s) in {checked}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
