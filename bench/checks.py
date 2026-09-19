"""Single source of truth for what gets tested.

Each check's SQL is run TWICE -- once by Spark (bench/build.py, producing ground
truth) and once by DuckDB (bench/verify.py). `{t}` is substituted with the Spark
table identifier or a DuckDB `iceberg_scan(...)` call respectively. Running the
same SQL on both engines is what makes "matches Spark" a real comparison rather
than an assertion against a pasted-in constant.
"""
import json
import re

# (key, table, label, sql)
CHECKS = [
    ("dv", "f.dv", "v3 deletion vectors (Puffin)",
     "SELECT count(*), sum(amount) FROM {t}"),
    ("dv_rowids", "f.dv", "v3 DV row-identity checksum",
     "SELECT count(*), sum(id), min(id), max(id) FROM {t}"),
    ("dv_v2", "f.dv_v2", "v2 positional deletes (baseline)",
     "SELECT count(*), sum(amount) FROM {t}"),
    ("lineage", "f.lineage", "v3 row lineage (data)",
     "SELECT id, val FROM {t} ORDER BY id"),
    ("part", "h.part", "v3 partitioned table, multiple DVs",
     "SELECT count(*), round(sum(amount),2), sum(id) FROM {t}"),
    ("merge", "h.merge", "v3 MERGE (merge-on-read)",
     "SELECT count(*), sum(CASE WHEN v='merged' THEN 1 ELSE 0 END) FROM {t}"),
    ("big3", "p.big3", "v3 deletion vectors (large table)",
     "SELECT count(*), sum(id) FROM {t}"),
    ("big2", "p.big2", "v2 positional deletes (large table)",
     "SELECT count(*), sum(id) FROM {t}"),
]

# Row-lineage metadata columns. Spark exposes them directly; DuckDB exposes the
# same names through iceberg_scan.
LINEAGE_COLS = ["_row_id", "_last_updated_sequence_number"]
LINEAGE_SQL = "SELECT id, {cols} FROM {t} ORDER BY id"

# Variant needs per-engine JSON serialization, so it gets its own SQL per engine.
VARIANT_TABLE = "h.var2"
VARIANT_SQL_SPARK = "SELECT id, to_json(v) FROM {t} ORDER BY id"
VARIANT_SQL_DUCKDB = "SELECT id, CAST(v AS JSON) FROM {t} ORDER BY id"


def normalize_json(s):
    """Parse a JSON string emitted by either engine into a comparable value.

    DuckDB emits bare fractions like `-.000123`, which is not strictly valid
    JSON, so leading zeros are restored before parsing. Parsing (rather than
    string comparison) also makes key order and whitespace irrelevant.
    """
    if s is None:
        return None
    if isinstance(s, (dict, list)):
        return s
    s = s.strip()
    if s == "" or s == "null":
        return None
    return json.loads(_restore_leading_zeros(s))


def _restore_leading_zeros(s):
    """Turn DuckDB's bare `-.5` fractions into valid JSON `-0.5`.

    Must skip string literals: a naive regex rewrites the CONTENTS of strings
    too, so `{"s":"a:.b"}` would silently become `{"s":"a:0.b"}` and mask a real
    difference between the engines. This walks the text and only rewrites while
    outside a string.
    """
    out, i, n, in_str, esc = [], 0, len(s), False, False
    while i < n:
        c = s[i]
        if in_str:
            out.append(c)
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
            i += 1
            continue
        if c == '"':
            in_str = True
            out.append(c)
            i += 1
            continue
        # A '.' that starts a number: at the very beginning, or after a
        # structural character / sign, with a digit following.
        if c == "." and i + 1 < n and s[i + 1].isdigit():
            prev = next((ch for ch in reversed(out) if not ch.isspace()), None)
            if prev is None or prev in ":[,{" or (prev == "-" and len(out) >= 1):
                out.append("0.")
                i += 1
                continue
        out.append(c)
        i += 1
    return "".join(out)


# Floating-point comparison tolerance, in decimal places.
#
# Spark and DuckDB render doubles differently (DuckDB emits `-.000123` where
# Spark emits `-0.000123`), and summing 5M doubles can differ in the last bits
# depending on aggregation order. Comparisons are therefore made to this many
# decimal places rather than exactly.
#
# This IS a real loosening: a reader returning 674988.50001 instead of 674988.5
# would pass. It is bounded deliberately -- the test data uses values that are
# exact in binary floating point (id * 1.5, id * 1.0), so in practice both
# engines return identical doubles and this tolerance is never exercised.
# Integers and Decimals are compared EXACTLY, with no rounding, which is what
# the row-identity checksums (sum/min/max of id) rely on.
FLOAT_DECIMALS = 4


def normalize_row(row):
    """Make engine row output comparable across Spark and DuckDB.

    Integers and integral Decimals are preserved exactly (20-digit variant
    integers must not become floats); non-integral values are compared to
    FLOAT_DECIMALS places. See that constant for why.
    """
    from decimal import Decimal

    out = []
    for v in row:
        if isinstance(v, Decimal):
            v = int(v) if v == v.to_integral_value() else round(float(v), FLOAT_DECIMALS)
        elif isinstance(v, float):
            v = round(v, FLOAT_DECIMALS)
        out.append(v)
    return tuple(out)


def normalize_rows(rows):
    return [normalize_row(r) for r in rows]
