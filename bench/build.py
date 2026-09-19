"""Build every Iceberg v3 test table with Spark, and record Spark's own answers
as ground truth in truth.json.

Nothing here is hardcoded from a previous run: snapshot IDs and expected results
are all measured at build time, so a fresh clone produces its own baseline.
"""
import argparse
import json
import os
import sys

from spark_setup import get_spark, WAREHOUSE, TRUTH_PATH, ICEBERG_VERSION
import checks as C

V3 = "'format-version'='3'"
V2 = "'format-version'='2'"
MOR = ("'write.delete.mode'='merge-on-read',"
       "'write.update.mode'='merge-on-read',"
       "'write.merge.mode'='merge-on-read'")

# v3 features we try to write and expect to fail with the current writers. These
# are ATTEMPTED at build time rather than asserted from a list, so the recorded
# reason is always this run's real error -- and if a future Spark/Iceberg gains
# support, this flips to writable on its own instead of silently staying stale.
UNWRITABLE_PROBES = [
    ("default_values", "ALTER TABLE ... ADD COLUMN ... DEFAULT", [
        "CREATE TABLE demo.h.probe_def (id BIGINT) USING iceberg TBLPROPERTIES ('format-version'='3')",
        "ALTER TABLE demo.h.probe_def ADD COLUMN status STRING DEFAULT 'active'",
    ]),
    ("geometry", "GEOMETRY column", [
        "CREATE TABLE demo.h.probe_geom (g GEOMETRY) USING iceberg TBLPROPERTIES ('format-version'='3')",
    ]),
    ("geography", "GEOGRAPHY column", [
        "CREATE TABLE demo.h.probe_geog (g GEOGRAPHY) USING iceberg TBLPROPERTIES ('format-version'='3')",
    ]),
    ("unknown", "UNKNOWN column", [
        "CREATE TABLE demo.h.probe_unk (u UNKNOWN) USING iceberg TBLPROPERTIES ('format-version'='3')",
    ]),
    ("multi_arg_transform", "PARTITIONED BY bucket(4, a, b)", [
        "CREATE TABLE demo.h.probe_mat (a BIGINT, b STRING) USING iceberg "
        "PARTITIONED BY (bucket(4, a, b)) TBLPROPERTIES ('format-version'='3')",
    ]),
]

# v3 types Spark accepts in DDL but may silently map to a different Iceberg type.
# (name, spark_type, expected_iceberg_type)
TYPE_PROBES = [
    ("timestamp_ns", "TIMESTAMP_NTZ", "timestamp_ns"),
    ("timestamptz_ns", "TIMESTAMP", "timestamptz_ns"),
]


def probe_unwritable(s):
    """Attempt each v3 feature; record whether it actually succeeded and why not."""
    out = {}
    # These probes are EXPECTED to raise. Spark 4 logs a full JSON stacktrace for
    # each one straight to stderr, which neither setLogLevel() nor the log4j2
    # Configurator suppresses; run_all.py filters those lines out of the output.
    probe_tables = ("probe_def", "probe_geom", "probe_geog", "probe_unk", "probe_mat")
    for key, what, stmts in UNWRITABLE_PROBES:
        for t in probe_tables:
            s.sql(f"DROP TABLE IF EXISTS demo.h.{t} PURGE")
        try:
            for st in stmts:
                s.sql(st)
            out[key] = {"writable": True, "what": what, "error": None}
        except Exception as e:
            out[key] = {"writable": False, "what": what,
                        "error": str(e).strip().splitlines()[0][:150]}
        print(f"  {key}: {'WRITABLE' if out[key]['writable'] else out[key]['error'][:80]}",
              flush=True)

    # Nanosecond timestamps are not an error case -- Spark accepts the DDL but
    # silently maps it to a different Iceberg type, so inspect what it produced.
    for key, spark_type, want in TYPE_PROBES:
        s.sql("DROP TABLE IF EXISTS demo.h.probe_ts PURGE")
        s.sql(f"CREATE TABLE demo.h.probe_ts (t {spark_type}) USING iceberg "
              f"TBLPROPERTIES ('format-version'='3')")
        got = json.load(open(newest_metadata("h.probe_ts")))["schemas"][-1]["fields"][0]["type"]
        out[key] = {
            "writable": got == want, "what": f"{spark_type} column",
            "error": None if got == want
            else f"Spark {spark_type} produced Iceberg type '{got}', not '{want}'",
        }
        print(f"  {key}: {spark_type} produced Iceberg type '{got}'", flush=True)

    # Table encryption keys (v3 `key-id` on snapshots) -- check whether requesting
    # encryption produces anything in the metadata.
    out["encryption_keys"] = {
        "writable": False, "what": "table encryption keys (v3 snapshot key-id)",
        "error": "not exercised by this harness -- needs a KMS/key-manager setup",
    }
    print("  encryption_keys: not exercised (needs a KMS)", flush=True)

    for t in probe_tables + ("probe_ts",):
        s.sql(f"DROP TABLE IF EXISTS demo.h.{t} PURGE")
    return out


def newest_metadata(table):
    import glob, re
    d = os.path.join(WAREHOUSE, *table.split("."), "metadata")
    files = glob.glob(os.path.join(d, "v*.metadata.json"))
    return max(files, key=lambda p: int(re.match(r"v(\d+)", os.path.basename(p)).group(1)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=5_000_000,
                    help="row count for the 5M-row performance tables")
    args = ap.parse_args()

    s = get_spark()
    s.sparkContext.setLogLevel("ERROR")
    for ns in ("f", "h", "p"):
        s.sql(f"CREATE NAMESPACE IF NOT EXISTS demo.{ns}")

    def build(name, ddl, steps):
        s.sql(f"DROP TABLE IF EXISTS demo.{name} PURGE")
        s.sql(ddl)
        for st in steps:
            s.sql(st)
        print(f"  built demo.{name}", flush=True)

    print("building v3 feature tables...", flush=True)

    # Deletion vectors: v3 merge-on-read delete -> Puffin DV
    build("f.dv",
          f"""CREATE TABLE demo.f.dv (id BIGINT, name STRING, amount DOUBLE)
              USING iceberg TBLPROPERTIES ({V3},{MOR})""",
          ["INSERT INTO demo.f.dv SELECT id, concat('row-',id), id*1.5 FROM range(1,1001)",
           "DELETE FROM demo.f.dv WHERE id % 10 = 0",
           "UPDATE demo.f.dv SET amount = -1 WHERE id = 7"])

    # Identical workload in v2 -> positional delete files, for comparison
    build("f.dv_v2",
          f"""CREATE TABLE demo.f.dv_v2 (id BIGINT, name STRING, amount DOUBLE)
              USING iceberg TBLPROPERTIES ({V2},{MOR})""",
          ["INSERT INTO demo.f.dv_v2 SELECT id, concat('row-',id), id*1.5 FROM range(1,1001)",
           "DELETE FROM demo.f.dv_v2 WHERE id % 10 = 0",
           "UPDATE demo.f.dv_v2 SET amount = -1 WHERE id = 7"])

    # Row lineage is always on in v3
    build("f.lineage",
          f"""CREATE TABLE demo.f.lineage (id BIGINT, val STRING)
              USING iceberg TBLPROPERTIES ({V3},{MOR})""",
          ["INSERT INTO demo.f.lineage VALUES (1,'a'),(2,'b'),(3,'c')",
           "UPDATE demo.f.lineage SET val='B-updated' WHERE id=2",
           "INSERT INTO demo.f.lineage VALUES (4,'d')"])

    # Partitioned, several delete rounds -> multiple DVs across partitions
    build("h.part",
          f"""CREATE TABLE demo.h.part (id BIGINT, cat STRING, amount DOUBLE)
              USING iceberg PARTITIONED BY (cat) TBLPROPERTIES ({V3},{MOR})""",
          ["INSERT INTO demo.h.part SELECT id, concat('c',id%4), id*1.0 FROM range(1,2001)",
           "DELETE FROM demo.h.part WHERE id % 7 = 0",
           "DELETE FROM demo.h.part WHERE id % 11 = 0",
           "DELETE FROM demo.h.part WHERE cat='c3' AND id < 500",
           "UPDATE demo.h.part SET amount = amount*10 WHERE id % 13 = 0"])

    # Deliberately awkward variant values
    build("h.var2",
          f"CREATE TABLE demo.h.var2 (id BIGINT, v VARIANT) USING iceberg TBLPROPERTIES ({V3})",
          ["""INSERT INTO demo.h.var2
              SELECT 1, parse_json('{"big":12345678901234567890,"neg":-0.000123,"s":"uni\\u00e9\\u4e2d","b":false,"nul":null}')
              UNION ALL SELECT 2, parse_json('[1,[2,[3,[4]]]]')
              UNION ALL SELECT 3, parse_json('{"arr":[{"k":1},{"k":2}],"empty":{},"ea":[]}')
              UNION ALL SELECT 4, NULL"""])

    build("h.merge",
          f"CREATE TABLE demo.h.merge (id BIGINT, v STRING) USING iceberg TBLPROPERTIES ({V3},{MOR})",
          ["INSERT INTO demo.h.merge SELECT id, concat('v',id) FROM range(1,101)",
           """MERGE INTO demo.h.merge t USING (SELECT id FROM range(1,101) WHERE id%5=0) u
              ON t.id = u.id WHEN MATCHED THEN UPDATE SET t.v='merged'"""])

    # Performance pair: same workload, v2 vs v3
    n = args.rows
    print(f"building {n:,}-row performance tables (this is the slow part)...", flush=True)
    for fv, props in ((2, V2), (3, V3)):
        build(f"p.big{fv}",
              f"""CREATE TABLE demo.p.big{fv} (id BIGINT, name STRING, amount DOUBLE)
                  USING iceberg TBLPROPERTIES ({props},{MOR})""",
              [f"INSERT INTO demo.p.big{fv} SELECT id, concat('row-',id), id*1.5 FROM range(1,{n+1})",
               f"DELETE FROM demo.p.big{fv} WHERE id % 10 = 0",
               f"DELETE FROM demo.p.big{fv} WHERE id % 17 = 0",
               f"DELETE FROM demo.p.big{fv} WHERE id % 23 = 0"])

    # ---- measure ground truth from Spark ----
    print("measuring Spark ground truth...", flush=True)
    truth = {
        "versions": {
            "spark": s.version,
            "iceberg": ICEBERG_VERSION,
            "rows_perf_table": n,
        },
        "checks": {},
    }

    print("probing v3 features the writers may not support...", flush=True)
    truth["unwritable"] = probe_unwritable(s)

    for key, table, label, sql in C.CHECKS:
        rows = s.sql(sql.format(t=f"demo.{table}")).collect()
        truth["checks"][key] = {
            "table": table, "label": label, "sql": sql,
            "rows": [list(C.normalize_row(tuple(r))) for r in rows],
        }
        print(f"  {label}: {truth['checks'][key]['rows']}", flush=True)

    # Row lineage metadata columns
    lin = s.sql(C.LINEAGE_SQL.format(t="demo.f.lineage", cols=", ".join(C.LINEAGE_COLS))).collect()
    truth["lineage"] = [list(C.normalize_row(tuple(r))) for r in lin]
    print(f"  row lineage: {truth['lineage']}", flush=True)

    # Variant, serialized to JSON and normalized
    var = s.sql(C.VARIANT_SQL_SPARK.format(t=f"demo.{C.VARIANT_TABLE}")).collect()
    truth["variant"] = [[r[0], C.normalize_json(r[1])] for r in var]
    print(f"  variant: {truth['variant']}", flush=True)

    # Snapshot IDs resolved at build time, each with its own row count
    snaps = s.sql("SELECT snapshot_id FROM demo.h.part.snapshots ORDER BY committed_at").collect()
    # Row counts alone would pass even if a reader returned the wrong ROWS for an
    # old snapshot, so record value-sensitive aggregates plus an ordered sample.
    truth["timetravel"] = []
    for r in snaps:
        agg = s.sql(f"""SELECT count(*) c, sum(id) sid, round(sum(amount),2) samt
                        FROM demo.h.part VERSION AS OF {r.snapshot_id}""").collect()[0]
        sample = s.sql(f"""SELECT id, cat, amount FROM demo.h.part VERSION AS OF {r.snapshot_id}
                           ORDER BY id LIMIT 5""").collect()
        truth["timetravel"].append({
            "snapshot_id": int(r.snapshot_id),
            "count": int(agg.c), "sum_id": int(agg.sid), "sum_amount": float(agg.samt),
            "sample": [list(C.normalize_row(tuple(x))) for x in sample],
        })
    print(f"  time travel: {[t['count'] for t in truth['timetravel']]} rows across "
          f"{len(truth['timetravel'])} snapshots", flush=True)

    # Delete-artifact layout: the structural v2-vs-v3 difference
    truth["layout"] = {}
    for fv in (2, 3):
        d = os.path.join(WAREHOUSE, "p", f"big{fv}", "data")
        files = [f for f in os.listdir(d) if not f.startswith(".")]
        dels = [f for f in files if "deletes" in f]
        truth["layout"][f"v{fv}"] = {
            # format-version read from the metadata Spark actually wrote, so
            # verify.py can assert the table really is v3 rather than trust the
            # TBLPROPERTIES we requested.
            "format_version": json.load(open(newest_metadata(f"p.big{fv}")))["format-version"],
            "data_files": len(files) - len(dels),
            "delete_artifacts": len(dels),
            "delete_bytes": sum(os.path.getsize(os.path.join(d, f)) for f in dels),
            "puffin": sum(1 for f in dels if f.endswith(".puffin")),
            "parquet_deletes": sum(1 for f in dels if f.endswith(".parquet")),
            "delete_suffix": sorted({f.split("-")[-1] for f in dels}),
        }
    print(f"  layout: {truth['layout']}", flush=True)

    with open(TRUTH_PATH, "w") as fh:
        json.dump(truth, fh, indent=2)
    print(f"\nwrote {TRUTH_PATH}")


if __name__ == "__main__":
    sys.exit(main())
