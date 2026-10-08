"""Read every table with DuckDB and compare against Spark's measured answers.

Expected values come from truth.json, which build.py produced by running the
same SQL through Spark. Nothing in this file is a hardcoded constant from a
previous run, including the snapshot IDs used for time travel.
"""
import argparse
import json
import os
import platform
import statistics
import sys
import time
import tempfile
import subprocess
import hashlib
from datetime import datetime, timezone

import duckdb

from spark_setup import metadata_json, TRUTH_PATH, WAREHOUSE, ROOT
import checks as C

PASS, FAIL, SKIP, GAP = "PASS", "FAIL", "n/a ", "GAP "
results = []


def record(label, status, detail=""):
    """GAP = a known, expected limitation -- reported, but not a suite failure."""
    results.append((label, status, detail))
    icon = {"PASS": "✓", "FAIL": "✗", SKIP: "-", GAP: "!"}[status]
    print(f"  {icon} {label:44} {status}  {detail}", flush=True)


def scan(table):
    return f"iceberg_scan('{metadata_json(table)}')"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", help="write a sanitized, shareable result JSON")
    args = ap.parse_args()
    results.clear()
    if not os.path.exists(TRUTH_PATH):
        print(f"missing {TRUTH_PATH} -- run: python bench/build.py", file=sys.stderr)
        return 2
    truth = json.load(open(TRUTH_PATH))
    # --verify-only invites running against a truth.json produced by an older
    # build.py. Fail with a clear instruction rather than a raw KeyError.
    required = ("versions", "checks", "lineage", "variant", "timetravel", "layout", "unwritable")
    missing = [k for k in required if k not in truth]
    if not missing and "live_delete_files" not in truth["layout"].get("v3", {}):
        missing = ["layout.v3.live_delete_files"]
    if missing or truth.get("schema_version") != 2:
        reason = "missing: " + ", ".join(missing) if missing else "expected schema_version=2"
        print(f"{TRUTH_PATH} is stale or from a different build.py ({reason}).\n"
              "Re-run: python run_all.py", file=sys.stderr)
        return 2

    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg;")
    extension = con.execute("SELECT extension_version, installed_from FROM duckdb_extensions() "
                            "WHERE extension_name='iceberg'").fetchone()
    print(f"\nDuckDB {duckdb.__version__}  vs  Spark {truth['versions']['spark']} "
          f"+ Iceberg {truth['versions']['iceberg']}\n"
          f"Iceberg extension {extension[0]} from {extension[1]}\n")

    print("READ CORRECTNESS (DuckDB result vs Spark result on identical SQL)")
    for key, table, label, sql in C.CHECKS:
        exp = [tuple(r) for r in truth["checks"][key]["rows"]]
        try:
            got = C.normalize_rows(con.execute(sql.format(t=scan(table))).fetchall())
        except Exception as e:
            record(label, FAIL, f"ERROR {type(e).__name__}: {str(e).splitlines()[0][:70]}")
            continue
        record(label, PASS if got == exp else FAIL,
               "" if got == exp else f"duckdb={got} spark={exp}")

    # Row lineage metadata columns
    exp = [tuple(r) for r in truth["lineage"]]
    try:
        got = C.normalize_rows(con.execute(
            C.LINEAGE_SQL.format(t=scan("f.lineage"), cols=", ".join(C.LINEAGE_COLS))
        ).fetchall())
        record("v3 row lineage (_row_id + _last_updated_seq)", PASS if got == exp else FAIL,
               "" if got == exp else f"duckdb={got} spark={exp}")
    except Exception as e:
        record("v3 row lineage (_row_id + _last_updated_seq)", FAIL,
               f"ERROR {str(e).splitlines()[0][:70]}")

    # Variant, compared as parsed JSON so key order and number formatting differences
    # between the two engines do not create false failures.
    exp = [[r[0], C.normalize_json(r[1])] for r in truth["variant"]]
    try:
        raw = con.execute(C.VARIANT_SQL_DUCKDB.format(t=scan(C.VARIANT_TABLE))).fetchall()
        got = [[r[0], C.normalize_json(r[1])] for r in raw]
        equal = C.json_equal(got, exp)
        record("v3 variant type (deep/awkward values)", PASS if equal else FAIL,
               "" if equal else f"duckdb={got} spark={exp}")
    except Exception as e:
        record("v3 variant type (deep/awkward values)", FAIL,
               f"ERROR {str(e).splitlines()[0][:70]}")

    # Separate try: a failure here must not re-record the label above, which
    # would report one PASS and one FAIL for the same check and silently drop
    # this one from the report card.
    try:
        vtype = con.execute(f"SELECT typeof(v) FROM {scan(C.VARIANT_TABLE)} LIMIT 1").fetchone()[0]
        record("v3 variant exposed as native type", PASS if vtype.upper() == "VARIANT" else FAIL,
               f"typeof() = {vtype}")
    except Exception as e:
        record("v3 variant exposed as native type", FAIL,
               f"ERROR {str(e).splitlines()[0][:70]}")

    # Time travel: snapshot IDs come from truth.json, generated by this clone's
    # build. Compare value-sensitive aggregates and an ordered sample, not just
    # row count -- a reader can return the right NUMBER of wrong rows.
    ok, detail = 0, ""
    for entry in truth["timetravel"]:
        sid = entry["snapshot_id"]
        src = f"iceberg_scan('{metadata_json('h.part')}', snapshot_from_id={sid})"
        try:
            agg = C.normalize_row(con.execute(
                f"SELECT count(*), sum(id), round(sum(amount),2) FROM {src}").fetchone())
            sample = C.normalize_rows(
                con.execute(f"SELECT id, cat, amount FROM {src} ORDER BY id LIMIT 5").fetchall()
                + con.execute(f"SELECT id, cat, amount FROM {src} ORDER BY id DESC LIMIT 5").fetchall())
            exp_agg = C.normalize_row((entry["count"], entry["sum_id"], entry["sum_amount"]))
            exp_sample = [tuple(r) for r in entry["sample"]]
            all_rows = C.normalize_rows(con.execute(
                f"SELECT id, cat, amount FROM {src} ORDER BY id").fetchall())
            exp_rows = [tuple(r) for r in entry["rows"]]
            if agg == exp_agg and sample == exp_sample and all_rows == exp_rows:
                ok += 1
            elif not detail:
                detail = (f"snapshot {sid}: duckdb agg={agg} spark={exp_agg}"
                          if agg != exp_agg else f"snapshot {sid}: rows differ")
        except Exception as e:
            if not detail:
                detail = f"snapshot {sid}: {type(e).__name__}: {str(e).splitlines()[0][:60]}"
    n_tt = len(truth["timetravel"])
    record("v3 time travel across DV snapshots", PASS if n_tt > 0 and ok == n_tt else FAIL,
           f"{ok}/{n_tt} snapshots match (aggregates + every row)" + (f" | {detail}" if detail else ""))

    # ---- v3 artifact validation ----
    # The read checks above would still pass if the writer silently fell back to
    # v2 positional deletes, so assert the on-disk representation explicitly.
    print("\nV3 ARTIFACT VALIDATION (what the writer actually produced)")
    v3, v2 = truth["layout"]["v3"], truth["layout"]["v2"]
    record("v3 table is really format-version 3", PASS if v3["format_version"] == 3 else FAIL,
           f"format-version={v3['format_version']}")
    # Filenames prove nothing, so this checks the Puffin footer's declared blob
    # types and the manifest's record of each delete file.
    dv_ok = (v3["dv_blob_count"] > 0
             and v3["non_dv_blob_count"] == 0
             and not v3["unreadable_puffin"])
    detail = (f"{v3['dv_blob_count']} live deletion-vector-v1 blob(s) in "
              f"{v3.get('live_puffin_files', '?')} Puffin file(s) "
              f"({v3['puffin']} on disk incl. files retained for older snapshots)")
    if v3["unreadable_puffin"]:
        detail += f" | UNREADABLE: {v3['unreadable_puffin'][0][:60]}"
    elif v3["non_dv_blob_count"]:
        detail += f" | non-DV blobs present: {v3['puffin_blob_types']}"
    record("v3 deletes are deletion-vector-v1 blobs", PASS if dv_ok else FAIL, detail)

    mf = v3.get("manifest_delete_files")
    if isinstance(mf, dict):
        record("v3 manifest records DVs as Puffin", FAIL, f"could not read: {mf.get('error')}")
    else:
        mf_ok = bool(mf) and all(e["file_format"] == "PUFFIN"
                                 and e["content"] == 1
                                 and e["has_referenced_data_file"] for e in mf)
        record("v3 manifest records DVs as Puffin", PASS if mf_ok else FAIL,
               f"{len(mf)} delete file(s), formats="
               f"{sorted({e['file_format'] for e in mf})}, all reference a data file="
               f"{all(e['has_referenced_data_file'] for e in mf) if mf else False}, "
               f"all position-delete content="
               f"{all(e['content'] == 1 for e in mf) if mf else False}")
    record("v2 baseline is really format-version 2", PASS if v2["format_version"] == 2 else FAIL,
           f"format-version={v2['format_version']}")
    v2mf = v2.get("manifest_delete_files")
    v2_formats = (sorted({e["file_format"] for e in v2mf})
                  if isinstance(v2mf, list) and v2mf else [])
    record("v2 deletes are positional delete files",
           PASS if (v2["parquet_deletes"] > 0 and v2["puffin"] == 0
                    and v2["dv_blob_count"] == 0 and v2_formats == ["PARQUET"]
                    and all(e["content"] == 1 for e in v2mf)) else FAIL,
           f"{v2['parquet_deletes']} parquet, {v2['puffin']} puffin, "
           f"0 DV blobs, manifest formats={v2_formats}")
    # NOTE: an earlier version of this check asserted "v3 merges DVs; v2
    # accumulates delete files" from on-disk file counts. That was wrong. The
    # snapshot history shows BOTH formats rewrite their delete artifacts each
    # round (added N, removed N, total N), so neither accumulates in this
    # workload. The on-disk difference includes retained historical files.
    record("neither format accumulates delete files",
           PASS if (C.stable_delete_history(v3) and C.stable_delete_history(v2)
                    and v3["live_delete_files"] == v2["live_delete_files"]) else FAIL,
           f"live delete artifacts: v3={v3['live_delete_files']} v2={v2['live_delete_files']} "
           f"(equal -- both rewrite per round, neither accumulates)")
    # Iceberg's added-delete-files counts delete ENTRIES (one per DV), not
    # physical files -- it reads 2 for both formats. The packaging difference is
    # in how many files those entries land in: v3 packs a commit's DV blobs into
    # one Puffin file, v2 writes one Parquet per data file.
    def per_commit(lay):
        hist = lay.get("snapshot_history")
        commits = sum(1 for h in hist if h["added_delete_files"]) if isinstance(hist, list) else 0
        return (lay["delete_artifacts"] / commits) if commits else None

    p3, p2 = per_commit(v3), per_commit(v2)
    record("v3 packs a commit's deletes into fewer files",
           PASS if (p3 is not None and p2 is not None and p3 < p2) else FAIL,
           f"physical delete files per commit: v3={p3} v2={p2} "
           f"(same {v3['live_delete_files']} delete entries each -- packaging, not accumulation)")

    # ---- write side: COPY TO against a local directory ----
    # Every claim the README makes about the local write path is exercised here.
    # Catalog writes need a running catalog and are outside this suite.
    print("\nLOCAL WRITE PATH: COPY TO (DuckDB)")
    import glob as _glob
    scratch = tempfile.TemporaryDirectory(prefix="write-probes-", dir=WAREHOUSE)

    def fresh(name):
        return os.path.join(scratch.name, name)

    def fmt_version(d):
        m = _glob.glob(os.path.join(d, "metadata", "*.metadata.json"))
        if not m:
            return None
        return json.load(open(max(m, key=os.path.getmtime)))["format-version"]

    def rows(d):
        m = _glob.glob(os.path.join(d, "metadata", "*.metadata.json"))
        return con.execute(
            f"SELECT count(*) FROM iceberg_scan('{max(m, key=os.path.getmtime)}')").fetchone()[0]

    # 1. FORMAT_VERSION 3 is requested but not honoured
    d = fresh("fv")
    try:
        con.execute(f"COPY (SELECT 1 a) TO '{d}' (FORMAT ICEBERG, FORMAT_VERSION 3)")
        fv = fmt_version(d)
        record("COPY TO honours FORMAT_VERSION 3", PASS if fv == 3 else GAP if fv == 2 else FAIL,
               f"asked for v3, got format-version={fv} (no error raised)")
    except Exception as e:
        record("COPY TO honours FORMAT_VERSION 3", FAIL, f"{type(e).__name__}: {e}")

    # 2. Unknown options are not rejected. Several are tried so the claim in the
    #    docs ("every option I tested was ignored") is backed by the suite.
    bogus = ["BANANA true", "THIS_IS_NOT_REAL 42", "FORMAT_VERSION 99", "VERSION 3",
             "OVERWRITE true", "NOT_AN_OPTION 'x'"]
    accepted = []
    for i, opt in enumerate(bogus):
        d = fresh(f"opt{i}")
        try:
            con.execute(f"COPY (SELECT 1 a) TO '{d}' (FORMAT ICEBERG, {opt})")
            accepted.append(opt.split()[0])
        except (duckdb.BinderException, duckdb.ParserException, duckdb.NotImplementedException):
            pass
        except Exception as e:
            record(f"unknown option probe {i}", FAIL, f"{type(e).__name__}: {e}")
    record("COPY TO rejects unknown options", GAP if accepted else PASS,
           f"{len(accepted)}/{len(bogus)} bogus options accepted silently: {accepted}"
           if accepted else "all rejected")

    # 3. APPEND true does not append -- it replaces the table
    d = fresh("app")
    try:
        con.execute(f"COPY (SELECT i AS id FROM range(5) t(i)) TO '{d}' (FORMAT ICEBERG)")
        before = rows(d)
        con.execute(f"COPY (SELECT 99 AS id) TO '{d}' (FORMAT ICEBERG, APPEND true)")
        after = rows(d)
        record("COPY TO ... APPEND true appends",
               PASS if after == before + 1 else GAP if before == 5 and after == 1 else FAIL,
               f"{before} rows + 1 appended -> {after} rows"
               + ("" if after == before + 1 else "  (table was REPLACED)"))
    except Exception as e:
        record("COPY TO ... APPEND true appends", FAIL, f"{type(e).__name__}: {e}")

    # 4. PARTITION_BY is ignored
    d = fresh("part")
    try:
        con.execute(f"COPY (SELECT i AS id, i%3 AS p FROM range(9) t(i)) TO '{d}' "
                    f"(FORMAT ICEBERG, PARTITION_BY p)")
        m = _glob.glob(os.path.join(d, "metadata", "*.metadata.json"))
        specs = json.load(open(max(m, key=os.path.getmtime)))["partition-specs"][0]["fields"]
        record("COPY TO ... PARTITION_BY partitions", PASS if specs else GAP,
               f"partition-spec fields = {specs}" + ("" if specs else "  (ignored)"))
    except Exception as e:
        record("COPY TO ... PARTITION_BY partitions", FAIL, f"{type(e).__name__}: {e}")

    # 5. No local/hadoop catalog for ATTACH
    try:
        con.execute(f"ATTACH '{WAREHOUSE}' AS _local (TYPE ICEBERG, ENDPOINT_TYPE 'HADOOP')")
        record("ATTACH supports a local (hadoop) catalog", PASS)
        con.execute("DETACH _local")
    except Exception as e:
        expected = "Unrecognized 'endpoint_type' (hadoop)" in str(e)
        record("ATTACH supports a local (hadoop) catalog", GAP if expected else FAIL,
               str(e).splitlines()[0][:92])
    scratch.cleanup()

    # ---- PyIceberg: reads v3, cannot write it ----
    # Automated so the claim in the docs isn't a hand-run anecdote.
    print("\nPYICEBERG 0.12 (third engine, cross-check)")
    try:
        import pyiceberg
        from pyiceberg.table import StaticTable
        exp_rows = truth["checks"]["dv"]["rows"][0][0]
        t = StaticTable.from_metadata(metadata_json("f.dv"))
        fv = t.metadata.format_version
        n = t.scan().to_arrow().num_rows
        record(f"PyIceberg {pyiceberg.__version__} reads a v3 table",
               PASS if (fv == 3 and n == exp_rows) else FAIL,
               f"format-version={fv}, {n} rows (spark: {exp_rows})")
    except Exception as e:
        record("PyIceberg reads a v3 table", FAIL,
               f"{type(e).__name__}: {str(e).splitlines()[0][:70]}")

    try:
        from pyiceberg.catalog.sql import SqlCatalog
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, LongType
        with tempfile.TemporaryDirectory(prefix="pyice_") as tmp:
            cat = SqlCatalog("t", uri=f"sqlite:///{tmp}/c.db", warehouse=f"file://{tmp}")
            cat.create_namespace_if_not_exists("ns")
            cat.create_table("ns.t", schema=Schema(NestedField(1, "id", LongType(), required=False)),
                             properties={"format-version": "3"})
            cat.engine.dispose()
        record("PyIceberg writes a v3 table", PASS, "unexpectedly succeeded")
    except NotImplementedError as e:
        record("PyIceberg writes a v3 table", GAP, str(e).splitlines()[0][:92])
    except Exception as e:
        record("PyIceberg writes a v3 table", FAIL,
               f"{type(e).__name__}: {str(e).splitlines()[0][:70]}")

    print("\nUNTESTED (outside this harness's writer route or setup)")
    for feat, info in truth["unwritable"].items():
        if info["writable"]:
            record(f"{feat} (now writable!)", GAP,
                   "writer gained support -- add a DuckDB read check for this")
        else:
            record(feat, SKIP, (info["error"] or "")[:92])

    # ---- performance ----
    nrows = truth["versions"]["rows_perf_table"]
    print(f"\nREAD PERFORMANCE ({nrows:,}-row tables, same delete workload)")

    def timed_scan(table):
        t0 = time.perf_counter()
        con.execute(f"SELECT count(*), sum(id) FROM {scan(table)}").fetchall()
        return (time.perf_counter() - t0) * 1000

    # These queries were already read during correctness checks. Warm the exact
    # timed query once more, then alternate order. This is a warm-cache benchmark.
    for table in ("p.big3", "p.big2"):
        timed_scan(table)
    timings = {"v3_ms": [], "v2_ms": [], "order": []}
    for i in range(7):
        order = ("p.big3", "p.big2") if i % 2 == 0 else ("p.big2", "p.big3")
        timings["order"].append(["v3" if t == "p.big3" else "v2" for t in order])
        for table in order:
            timings["v3_ms" if table == "p.big3" else "v2_ms"].append(timed_scan(table))
    v3ms, v2ms = statistics.median(timings["v3_ms"]), statistics.median(timings["v2_ms"])
    truth["performance"] = {
        "rows": nrows,
        "duckdb": duckdb.__version__,
        "iceberg_extension": {"version": extension[0], "source": extension[1]},
        "python": platform.python_version(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu": cpu_name(),
        "threads": con.execute("SELECT current_setting('threads')").fetchone()[0],
        "measured_at_utc": datetime.now(timezone.utc).isoformat(),
        "cache_state": "warm; correctness queries precede one extra warmup of the timed query",
        "warmups_per_table": 1,
        "timings_ms": timings,
    }
    with open(TRUTH_PATH, "w") as fh:
        json.dump(truth, fh, indent=2)
    lay = truth["layout"]
    print(f"  v3 deletion vectors (Puffin)   median {v3ms:7.1f} ms   "
          f"{lay['v3']['live_delete_files']} live entries in {lay['v3']['live_physical_delete_files']} file(s)")
    print(f"  v2 positional deletes          median {v2ms:7.1f} ms   "
          f"{lay['v2']['live_delete_files']} live entries in {lay['v2']['live_physical_delete_files']} file(s)")
    print(f"  -> v3 is {v2ms/v3ms:.1f}x faster on this machine")
    print(f"  raw timings written to {TRUTH_PATH}")

    # ---- summary ----
    n_fail = sum(1 for _, s, _ in results if s == FAIL)
    n_pass = sum(1 for _, s, _ in results if s == PASS)
    n_skip = sum(1 for _, s, _ in results if s == SKIP)
    n_gap = sum(1 for _, s, _ in results if s == GAP)
    print(f"\n{n_pass} passed, {n_fail} failed, {n_gap} known gaps, {n_skip} untested")
    truth["results"] = [{"label": label, "status": status.strip(), "detail": detail}
                        for label, status, detail in results]
    if args.results:
        publish_results(truth, args.results)
    con.close()
    # Observed gaps and untested features are informational; failures are not.
    return 1 if n_fail else 0


def cpu_name():
    if sys.platform == "darwin":
        try:
            return subprocess.check_output(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                text=True, stderr=subprocess.DEVNULL).strip()
        except (OSError, subprocess.CalledProcessError):
            return "unavailable (CPU query denied or failed)"
    return platform.processor() or platform.machine()


def publish_results(truth, path):
    """Keep raw evidence, redact local paths, and fingerprint the measured code."""
    import pathlib
    import importlib.metadata
    code = [pathlib.Path(ROOT, "run_all.py")]
    for directory in ("bench", "tests", "tools"):
        code.extend(sorted(pathlib.Path(ROOT, directory).glob("*.py")))
    truth["provenance"] = {
        "code_sha256": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in code},
        "packages": dict(sorted((d.metadata["Name"], d.version)
                                for d in importlib.metadata.distributions())),
        "java": subprocess.run(["java", "-version"], capture_output=True, text=True, check=True).stderr.strip(),
    }
    def redact(value):
        if isinstance(value, str):
            for source in (os.path.realpath(ROOT), ROOT):
                value = value.replace(source, "<repo>")
            return value
        if isinstance(value, list):
            return [redact(v) for v in value]
        if isinstance(value, dict):
            return {k: redact(v) for k, v in value.items()}
        return value
    target = pathlib.Path(path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(redact(truth), indent=2) + "\n", encoding="utf-8")
    print(f"  shareable results written to {target}")


if __name__ == "__main__":
    sys.exit(main())
