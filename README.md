# Iceberg v3 × DuckDB compatibility bench

A reproducible bench that writes Apache Iceberg **v3** tables with Spark, reads them
back with **DuckDB**, and checks whether DuckDB gets the same answers Spark does.

It's the code behind the article *"Testing Iceberg v3 Against DuckDB: Deletion
Vectors, Row Lineage, and Variant."*

> **Scope.** This harness does **not** cover all of Iceberg v3. It exercises three
> of the spec's capabilities end-to-end — deletion vectors, row lineage, and the
> variant type — and explicitly reports the rest as untested, with the reason, on
> every run. See [Coverage](#coverage) for the full list.

## Result

With **DuckDB 1.5.5**, **Iceberg 1.11.0**, **Spark 4.0.4**:

```
READ CORRECTNESS (DuckDB result vs Spark result on identical SQL)
  ✓ v3 deletion vectors (Puffin)                 PASS
  ✓ v3 DV row-identity checksum                  PASS
  ✓ v2 positional deletes (baseline)             PASS
  ✓ v3 row lineage (data)                        PASS
  ✓ v3 partitioned table, multiple DVs           PASS
  ✓ v3 MERGE (merge-on-read)                     PASS
  ✓ v3 deletion vectors (large table)            PASS
  ✓ v2 positional deletes (large table)          PASS
  ✓ v3 row lineage (_row_id + _last_updated_seq) PASS
  ✓ v3 variant type (deep/awkward values)        PASS
  ✓ v3 variant exposed as native type            PASS  typeof() = VARIANT
  ✓ v3 time travel across DV snapshots           PASS  5/5 (count+sum_id+sum_amount+sample)

V3 ARTIFACT VALIDATION (what the writer actually produced)
  ✓ v3 table is really format-version 3          PASS  format-version=3
  ✓ v3 deletes are deletion-vector-v1 blobs      PASS  6 DV blobs in 3 Puffin file(s), 0 parquet
  ✓ v3 manifest records DVs as Puffin            PASS  formats=['PUFFIN'], all reference a data file
  ✓ v2 baseline is really format-version 2       PASS  format-version=2
  ✓ v2 deletes are positional delete files       PASS  6 parquet, 0 DV blobs, manifest=['PARQUET']
  ✓ v3 merges DVs; v2 accumulates delete files   PASS  v3=3 vs v2=6 artifacts

LOCAL WRITE PATH: COPY TO (DuckDB)
  ! COPY TO honours FORMAT_VERSION 3             GAP   asked v3, got format-version=2
  ! COPY TO rejects unknown options              GAP   'BANANA true' accepted silently
  ! COPY TO ... APPEND true appends              GAP   5 rows + 1 -> 1 row (REPLACED)
  ! COPY TO ... PARTITION_BY partitions          GAP   partition-spec fields = [] (ignored)
  ! ATTACH supports a local (hadoop) catalog     GAP   accepted: glue, s3_tables
```

**For the three v3 features it covers, DuckDB matches Spark exactly** — deletion
vectors, row lineage, and the variant type, plus time travel across DV snapshots.
On 5M rows with ~19% deleted, v3 deletion vectors read **~4× faster** than the
equivalent v2 positional deletes (3.6–4.7× across six runs on one laptop; v3 is
steady at ~41 ms, the v2 number wanders between 145 and 195 ms).

This is a statement about the features listed under [Coverage](#coverage), not
about Iceberg v3 as a whole.

On the **write** side DuckDB has two very different modes:

- **With a catalog** (`ATTACH ... TYPE ICEBERG` against REST/Glue/S3 Tables):
  `INSERT`/`UPDATE`/`DELETE` all work, and writing to a v3 table produces **real
  Puffin deletion vectors** that Spark reads back correctly. Verified round-trip
  against a local `apache/iceberg-rest-fixture`.
- **Without a catalog** (`COPY TO` a local directory): create-only, v2 only, and
  **unknown option names are accepted without error** — a made-up `BANANA true`
  is accepted just as happily as `FORMAT_VERSION 3`. Of the options tested,
  none were honoured: `APPEND true` does not append, it *replaces* the table,
  and `PARTITION_BY` is ignored. There is no local/`hadoop` catalog for `ATTACH`.

Practical rule: **if you want DuckDB to write Iceberg, give it a catalog.**

**Automated vs manual.** Everything in the output above is automated, including
all five `COPY TO` findings. The **catalog** write findings (`ATTACH` →
`INSERT`/`UPDATE`/`DELETE` producing real deletion vectors) are **manual** — they
need a running REST catalog, which this suite deliberately doesn't require. See
[Manual findings](#manual-findings-not-covered-by-the-suite) for the reproduction.

## Coverage

Iceberg v3 adds roughly nine capabilities. This harness exercises three of them
end-to-end and reports the rest, with the blocking reason, on every run.

| v3 capability | Status | Notes |
|---|---|---|
| Binary deletion vectors | ✅ **Tested** | Written by Spark, read by DuckDB, artifacts asserted as Puffin |
| Row lineage | ✅ **Tested** | `_row_id` + `_last_updated_sequence_number` compared against Spark |
| `variant` type | ✅ **Tested** | Deep nesting, 20-digit ints, unicode, empty containers, NULL |
| Default values | ⚠️ Untested | Spark can't write them (`setting default values ... unsupported`) |
| `geometry` / `geography` | ⚠️ Untested | Spark SQL parser: `[UNSUPPORTED_DATATYPE]` |
| `unknown` type | ⚠️ Untested | Spark SQL parser: `[UNSUPPORTED_DATATYPE]` |
| `timestamp_ns` / `timestamptz_ns` | ⚠️ Untested | Spark maps these to `timestamp` / `timestamptz` (microseconds) |
| Multi-argument transforms | ⚠️ Untested | Iceberg-Spark: `Cannot convert transform with more than one column reference` |
| Table encryption keys | ⚠️ **Not attempted** | Needs a KMS/key-manager; the harness never tries this one |

**These blockers are specific to the writer this harness uses** — Spark 4.0.4 with
`iceberg-spark-runtime-4.0_2.13:1.11.0`. They are not claims about Iceberg v3
support in general: Trino, Flink, or a newer Iceberg release may write some of
these fine. Encryption keys are the one row that was never attempted at all
(it needs a KMS), as opposed to attempted and refused.

Everything else in the ⚠️ column is **probed on every run**, not hardcoded — if a
future Spark or Iceberg release gains support, the row flips to
`GAP  writer gained support` instead of silently repeating stale news.
Hand-forging metadata to fake these would prove nothing about real pipelines, so
they're reported rather than graded.

If your interest in v3 is geospatial types or default values, **check your writer
before blaming your reader.**

## Running it

Requires a JDK (Spark needs one) and Python 3.9+.

```bash
brew install openjdk@21          # or any JDK 17/21; set JAVA_HOME if not Homebrew
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

python run_all.py                # full run, 5M-row perf tables (~5-10 min)
python run_all.py --rows 200000  # much faster, smaller perf gap
python run_all.py --verify-only  # re-run DuckDB checks against existing tables
```

(Use `python3` to create the venv; inside the activated venv, `python` works.)

Spark downloads the Iceberg runtime JAR from Maven on first run.

Exit code is non-zero only if DuckDB **disagrees with Spark**. Known gaps and
untestable features are reported but don't fail the run.

## How the comparison works

The thing that makes this trustworthy is that expected values are never
hardcoded. Each check in `bench/checks.py` defines one SQL string, which is run
**twice**:

- `bench/build.py` runs it through **Spark** and writes the answer to `truth.json`
- `bench/verify.py` runs the same SQL through **DuckDB** and compares

So `PASS` means two independent engines agreed on the same query over the same
table — not that a result matched a constant someone pasted in. `truth.json` is
gitignored and regenerated per run, and snapshot IDs for the time-travel test are
resolved at build time (they're generated fresh on every commit, so hardcoding
them would break for everyone but the original author).

Features Spark cannot produce in this harness are **probed at build time**, not
assumed. If a future Spark or Iceberg release gains support, the run flips that
row to `GAP  writer gained support` instead of quietly reporting stale news.
(Table encryption keys are the one exception: never attempted, so reported as
untried rather than probed.)

## What's tested

| Area | Table | What it proves |
|---|---|---|
| Deletion vectors | `f.dv` | v3 merge-on-read delete writes a Puffin DV; DuckDB applies it |
| Row identity | `f.dv` | `count/sum/min/max(id)` — catches a DV applied at the wrong offset |
| v2 baseline | `f.dv_v2` | Identical workload as positional deletes, for comparison |
| Row lineage | `f.lineage` | `_row_id` survives an update; `_last_updated_sequence_number` bumps |
| Partitioning | `h.part` | Multiple DVs across partitions, overlapping delete predicates |
| Variant | `h.var2` | 20-digit ints, negative decimals, unicode, 4-deep nesting, empty containers, NULL |
| Time travel | `h.part` | Older snapshots apply the DVs as of *that* commit — compared on count, `sum(id)`, `sum(amount)` and an ordered sample, so returning the right *number* of wrong rows fails |
| MERGE | `h.merge` | v3 merge-on-read MERGE |
| Artifact validation | `p.big2/3` | The v3 table really is format-version 3, and its delete artifacts are verified as deletion vectors two independent ways: the **Puffin footer is parsed** and every blob must be `deletion-vector-v1`, and the **manifest** (`.delete_files`) must record them as `PUFFIN` with a `referenced_data_file`. Converse for the v2 baseline. A `.puffin` filename is never treated as proof |
| Local write path | `_dw_*` | All five `COPY TO` / `ATTACH` gaps, asserted rather than described |
| Performance | `p.big2/3` | Same workload, v2 vs v3, median of 7 runs |

Artifact validation matters because the read checks alone would still pass if the
writer silently fell back to v2 positional deletes — the query results would be
identical. The suite asserts the on-disk representation separately.

### Float comparison

Integers and integral Decimals are compared **exactly** — the row-identity
checksums depend on that, and the 20-digit variant integer must not become a
float. Non-integral values are compared to `FLOAT_DECIMALS = 4` places
(`bench/checks.py`), because the two engines render doubles differently and
summing 5M of them can differ in the last bits by aggregation order. The test
data uses values that are exact in binary floating point (`id * 1.5`, `id * 1.0`),
so in practice both engines return identical doubles and the tolerance is never
exercised — but it is a real loosening and is documented as such.

## Manual findings (not covered by the suite)

These were verified by hand and are **not** asserted by `run_all.py`, because they
need a running REST catalog:

With a catalog attached, DuckDB does full `INSERT`/`UPDATE`/`DELETE`, and writing
into a **Spark-created v3 table** produces **real Puffin deletion vectors** that
Spark reads back correctly.

**1. Start the catalog.** The bind-mount matters: without it the catalog writes
metadata inside the container at a path DuckDB then can't resolve on the host.

```bash
mkdir -p /tmp/ice-wh && chmod 777 /tmp/ice-wh
docker run -d --name ice-rest -p 8181:8181 \
  -v /tmp/ice-wh:/tmp/ice-wh \
  -e CATALOG_WAREHOUSE=/tmp/ice-wh \
  -e CATALOG_IO__IMPL=org.apache.iceberg.hadoop.HadoopFileIO \
  apache/iceberg-rest-fixture@sha256:db8de90b5b7693d4ac334c336f91d9bbe320d7b19f4f514d26de84cdfbcbfe8d
# that digest is what was tested; apache/iceberg-rest-fixture:1.10.1 is the
# newest version tag if you prefer one.
```

**2. Create the v3 table with Spark**, pointed at the same catalog:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .config("spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.rest.type", "rest")
    .config("spark.sql.catalog.rest.uri", "http://localhost:8181")
    .config("spark.sql.catalog.rest.warehouse", "/tmp/ice-wh")
    .master("local[2]")
    .getOrCreate()
)
```

```sql
CREATE NAMESPACE IF NOT EXISTS rest.v3ns;
CREATE TABLE rest.v3ns.t3 (id BIGINT, name STRING) USING iceberg
  TBLPROPERTIES ('format-version'='3',
                 'write.delete.mode'='merge-on-read',
                 'write.update.mode'='merge-on-read');
INSERT INTO rest.v3ns.t3 SELECT id, concat('v',id) FROM range(1,11);
DELETE FROM rest.v3ns.t3 WHERE id = 5;          -- 9 rows remain
```

**3. Write to it from DuckDB:**

```sql
INSTALL iceberg; LOAD iceberg;
ATTACH '' AS ice (TYPE ICEBERG, ENDPOINT 'http://localhost:8181',
                  AUTHORIZATION_TYPE 'none');
INSERT INTO ice.v3ns.t3 VALUES (99, 'from-duckdb');
DELETE FROM ice.v3ns.t3 WHERE id = 2;
UPDATE ice.v3ns.t3 SET name = 'changed' WHERE id = 3;
```

**4. Check the artifacts.** The table stays at `format-version: 3` with **3 Puffin
files and 0 Parquet delete files**, two of the Puffin files written by DuckDB —
its files use bare UUIDv7 names (`966d0efc-…-deletes.puffin`) where Spark's are
task-numbered (`00000-211-…-deletes.puffin`).

**5. Read back from Spark — in a NEW process.** This step has a trap worth knowing
about: Iceberg's Spark catalog caches table metadata, so the session from step 2
will happily keep serving the pre-DuckDB state and make it look like nothing was
written. Start a fresh Spark process (or set
`spark.sql.catalog.rest.cache-enabled=false`), then:

```sql
SELECT id, name FROM rest.v3ns.t3 ORDER BY id;
```

Expected — 9 rows, with DuckDB's insert, delete and update all round-tripped
through deletion vectors:

```
(1,'v1') (3,'changed') (4,'v4') (6,'v6') (7,'v7') (8,'v8') (9,'v9') (10,'v10') (99,'from-duckdb')
        ^ updated by DuckDB                                            ^ inserted by DuckDB
id=2 absent (deleted by DuckDB)   id=5 absent (deleted by Spark in step 2)
```

```bash
docker rm -f ice-rest && rm -rf /tmp/ice-wh   # cleanup
```

These steps were run end-to-end against the pinned digest above; the output shown
is from that run.

## Why Spark and not PyIceberg

PyIceberg 0.12.0 can *read* v3 but cannot *write* it:

```
NotImplementedError: Writing V3 is not yet supported
# https://github.com/apache/iceberg-python/issues/1551
```

Its v3 types (`UnknownType`, `GeometryType`, `TimestampNanoType`, `FileFormat.PUFFIN`)
are all defined, and its reader handles v3 fine — only the writer is missing. So
Spark + `iceberg-spark-runtime-4.0_2.13:1.11.0` is the writer here.

## Layout

```
run_all.py            build + verify
bench/checks.py       shared check definitions (the SQL both engines run)
bench/build.py        Spark: build tables, probe writer limits, emit truth.json
bench/verify.py       DuckDB: run the same SQL, compare, print the report card
bench/spark_setup.py  Spark session + metadata path resolution
```

## Caveats

The ~4× speedup was measured on a laptop, on local disk, on a scan-heavy query.
Don't quote it as a universal number — the *direction* is structural (DVs merge
to one per data file; positional deletes accumulate per delete operation), but
the magnitude depends on your delete pattern, file sizes, and storage. It also
shrinks on small tables: at 200k rows the same bench shows ~2×.

## License

MIT
