# Iceberg v3 × DuckDB compatibility bench

A reproducible bench that writes Apache Iceberg **v3** tables with Spark, reads them
back with **DuckDB**, and checks whether DuckDB gets the same answers Spark does.

It's the code behind the article *"I Tested Every Iceberg v3 Feature Against DuckDB."*

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
  ✓ v3 row lineage (_row_id, _seq_number)        PASS
  ✓ v3 variant type (deep/awkward values)        PASS
  ✓ v3 variant exposed as native type            PASS  typeof() = VARIANT
  ✓ v3 time travel across DV snapshots           PASS  5/5 snapshots match

WRITE SUPPORT (DuckDB)
  ! DuckDB writes Iceberg v3 on request          GAP   asked for v3, got format-version=2 (no error raised)
```

**DuckDB reads Iceberg v3 correctly**, including deletion vectors, row lineage,
the variant type, and time travel across DV snapshots. On 5M rows with ~19%
deleted, v3 deletion vectors read **~4.5× faster** than the equivalent v2
positional deletes (4.3–4.7× across runs on one laptop).

On the **write** side DuckDB has two very different modes:

- **With a catalog** (`ATTACH ... TYPE ICEBERG` against REST/Glue/S3 Tables):
  `INSERT`/`UPDATE`/`DELETE` all work, and writing to a v3 table produces **real
  Puffin deletion vectors** that Spark reads back correctly. Verified round-trip
  against a local `apache/iceberg-rest-fixture`.
- **Without a catalog** (`COPY TO` a local directory): create-only, v2 only, and
  the option list is **not validated at all** — `BANANA true` is accepted just as
  happily as `FORMAT_VERSION 3`. `APPEND true` does not append; it replaces the
  table. There is no local/`hadoop` catalog for `ATTACH`.

Practical rule: **if you want DuckDB to write Iceberg, give it a catalog.**

The automated suite in this repo covers the read side and the `COPY TO` gap. The
catalog write findings were verified manually (see the article); a scripted version
would need a REST catalog container, which the suite deliberately doesn't require.

## Running it

Requires a JDK (Spark needs one) and Python 3.9+.

```bash
brew install openjdk@21          # or any JDK 17/21; set JAVA_HOME if not Homebrew
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

python run_all.py                # full run, 5M-row perf tables (~5-10 min)
python run_all.py --rows 200000  # much faster, smaller perf gap
python run_all.py --verify-only  # re-run DuckDB checks against existing tables
```

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

Features no available writer can produce are **probed at build time**, not
assumed. If a future Spark or Iceberg release gains support, the run flips that
row to `GAP  writer gained support` instead of quietly reporting stale news.

## What's tested

| Area | Table | What it proves |
|---|---|---|
| Deletion vectors | `f.dv` | v3 merge-on-read delete writes a Puffin DV; DuckDB applies it |
| Row identity | `f.dv` | `count/sum/min/max(id)` — catches a DV applied at the wrong offset |
| v2 baseline | `f.dv_v2` | Identical workload as positional deletes, for comparison |
| Row lineage | `f.lineage` | `_row_id` survives an update; `_last_updated_sequence_number` bumps |
| Partitioning | `h.part` | Multiple DVs across partitions, overlapping delete predicates |
| Variant | `h.var2` | 20-digit ints, negative decimals, unicode, 4-deep nesting, empty containers, NULL |
| Time travel | `h.part` | Reading older snapshots applies the DVs as of *that* commit |
| MERGE | `h.merge` | v3 merge-on-read MERGE |
| Performance | `p.big2/3` | Same workload, v2 vs v3, median of 7 runs |

## Untested, and why

Three v3 features aren't graded here because **no available writer can produce them**:

| Feature | Blocker |
|---|---|
| Column default values | Spark: `Cannot add column ... setting default values in Spark is currently unsupported` |
| `geometry` / `geography` | Spark SQL parser: `[UNSUPPORTED_DATATYPE]` |
| `timestamp_ns` | Spark `TIMESTAMP_NTZ` maps to Iceberg `timestamp` (microseconds) |

Hand-forging metadata to fake these would prove nothing about real pipelines, so
they're reported as untested rather than graded. If your interest in v3 is
geospatial types or default values, **the writers are the blocker, not DuckDB.**

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

The ~4.5× speedup was measured on a laptop, on local disk, on a scan-heavy query.
Don't quote it as a universal number — the *direction* is structural (DVs merge
to one per data file; positional deletes accumulate per delete operation), but
the magnitude depends on your delete pattern, file sizes, and storage. It also
shrinks on small tables: at 200k rows the same bench shows ~1.8×.

## License

MIT
