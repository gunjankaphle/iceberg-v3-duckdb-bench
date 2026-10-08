# Iceberg v3 × DuckDB compatibility bench

A local experiment that writes Apache Iceberg tables with Spark and checks
DuckDB's answers against Spark's answers. It covers **deletion vectors, row
lineage, and variant values**, plus partitioning, merge-on-read updates, and
time travel. It is not a certification of the entire v3 specification.

The publication run passed **24 checks**, with zero failures, six observed
gaps, and eight untested cases. On its 5M-row warm scan, v3 took **40.5 ms**
versus **148.4 ms** for v2: **3.7×** faster on that machine. See the saved
evidence and measurement limits below.

## Run it

Requires **Python 3.10+ and JDK 17 or 21**. Python 3.11 and JDK 21 are used in CI.
The pinned engines are DuckDB 1.5.6, Spark 4.0.4, Iceberg 1.11.0, and
PyIceberg 0.12.0.

```bash
git clone https://github.com/gunjankaphle/iceberg-v3-duckdb-bench
cd iceberg-v3-duckdb-bench
# macOS; on other platforms install a JDK through your package manager
brew install openjdk@21
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt

python run_all.py --rows 200000       # quicker check
python run_all.py                     # 5M-row performance tables
python run_all.py --verify-only       # existing tables + truth.json required
```

The Spark setup honors `JAVA_HOME`, or finds Homebrew's OpenJDK 21 installation.
On other installations, set `JAVA_HOME` to your JDK directory. The first run
needs network access to download the Iceberg JAR and DuckDB extension.

**The warehouse is disposable.** A build purges and recreates this harness's
tables under `warehouse/` and replaces `truth.json`. Do not put personal data
there. Local DuckDB write probes use their own temporary directory and remove
only that directory. Full subprocess logs are saved under `logs/`, and a failed
subprocess prints its last 40 lines.

`--rows` must be at least 1,000. The default 5M-row build can take several
minutes. Exit status is nonzero for failed comparisons, invalid artifacts,
unexpected probe errors, build failures, or failed regression/doc checks.
Observed limitations are reported as `GAP`; uncovered features as `n/a`.
Neither is a successful compatibility test.

## What is checked

| Area | Evidence |
|---|---|
| v3 deletion vectors | Spark-produced data, updates and deletes; all 900 surviving rows compared |
| v2 positional deletes | Same small workload and full-row comparison |
| Row lineage | All data rows and `_row_id` / `_last_updated_sequence_number` compared |
| Partitioned v3 table | All surviving rows compared after overlapping deletes and an update |
| MERGE | All 100 resulting rows compared, including updated payloads |
| Time travel | Every row compared in each of five partitioned-table snapshots, plus aggregates |
| Variant | JSON values compared with exact decimal parsing and boolean/number distinction; native `VARIANT` exposure checked separately |
| Large tables | `count(*)` and `sum(id)` compared; **not** a full-row comparison |
| Artifacts | Metadata format version, delete manifests, and Puffin footer blob types checked for the large-table pair |
| Delete history | Three delete rounds must replace entries rather than accumulate them |
| Local writes | Format version, unknown options, append, partitioning, and attempted Hadoop catalog attachment |
| PyIceberg | Reads the v3 delete fixture and matches its row count; attempts v3 table creation |

Shared SQL in `bench/checks.py` runs through Spark to generate the baseline and
through DuckDB to compare. Variant serialization is engine-specific, but the
parsed values are compared by the same code. Spark is the reference for this
experiment, not an independent proof of specification compliance.

Integers are exact. Ordinary SQL floating-point results are rounded to four
decimal places before comparison. The fixtures use binary-exact amounts such
as `id * 1.5`; that does not make the tolerance disappear. Variant decimal values
are compared exactly, without that rounding. The JSON comparison checks the
tested values, not every possible variant subtype or physical encoding.

The artifact check follows each live manifest entry's `content_offset` into its
Puffin footer and requires `deletion-vector-v1`. A filename alone is not evidence
of a DV. Manifest delete entries and physical files are counted separately:
two DVs may occupy one Puffin file. Historical files retained for earlier
snapshots are not described as orphans.

## Results and reproducibility

The current DuckDB 1.5.6 verification run is saved in
[results/publication-duckdb-1.5.6.json](results/publication-duckdb-1.5.6.json).
It uses Iceberg extension build `890b78a9c` and reuses the existing Spark-built
fixtures; Spark writer probes and layout evidence were not regenerated.
The earlier 1.5.5 run remains in [results/publication.json](results/publication.json).
Supplementary 1.5.6 lineage, append, and before/after-delete checks are saved in
[results/article-probes-duckdb-1.5.6.json](results/article-probes-duckdb-1.5.6.json).
It includes Spark baselines, layout evidence, check verdicts, seven raw timing
pairs, engine and extension versions, CPU, Java, Python, thread count, UTC
measurement time, and SHA-256 fingerprints of the benchmark source files.
Local repository paths are replaced with `<repo>`; the export is evidence, not
a portable replacement for the local warehouse or `truth.json`.

Save another run without losing earlier evidence:

```bash
python run_all.py --results results/my-run.json
python run_all.py --verify-only --results results/my-reread.json
```

These commands overwrite the named export if it exists; choose distinct names
when collecting multiple runs. An existing `truth.json` from an older schema
requires a rebuild.

The Iceberg extension is installed from DuckDB's core repository and its build
is recorded. Pinning the DuckDB Python package alone does not pin every possible
extension update. Compare the recorded extension build when reproducing a result.
Top-level Python dependencies are pinned; transitive dependencies are reported
by the installed environment rather than locked here.

The performance query is `SELECT count(*), sum(id)`. Correctness checks read both
tables first, then the exact timed query gets one extra warmup per table. Seven
rounds alternate v3-first and v2-first order, on one DuckDB connection. These are
**warm local scans**, including query planning and metadata resolution, not
cold-start or object-storage measurements. The ratio is the median v2 time divided
by the median v3 time. Performance never determines the suite's exit status.

The observed advantage cannot be assigned solely to the bitmap encoding: this
writer also packs two live DVs into one Puffin file, versus two live Parquet
position-delete files. Reader implementation, file packaging, and cache state
can contribute. Equal delete-entry counts do not imply equal file-open costs.

## Observed local write limitations

On the tested extension, the automated probes found:

- `FORMAT_VERSION 3` was accepted but produced a v2 table.
- All six option probes were accepted without an error, including
  `BANANA true` and `FORMAT_VERSION 99`.
- `APPEND true` replaced a five-row table with the single new row.
- `PARTITION_BY p` produced an empty partition specification.
- `ENDPOINT_TYPE 'HADOOP'` was rejected.

These describe the tested calls, not every option or catalog DuckDB supports.
Replacement means the current table lost its previous rows; the suite does not
establish whether the underlying old files can be recovered. Treat local
`COPY TO` as an export path and verify the resulting metadata before relying
on an option.

Catalog writes are **outside the automated suite**. An earlier manual experiment
against `apache/iceberg-rest-fixture` observed DuckDB `INSERT`, `DELETE`, and
`UPDATE` into a Spark-created v3 table, with the resulting rows visible to Spark.
That experiment's evidence is not included in the publication export and was
not repeated for the final review. It does not establish behavior for Glue or
S3 Tables, nor does filename inspection alone prove the written blobs were DVs.
For catalog use, consult [DuckDB's Iceberg documentation](https://duckdb.org/docs/stable/core_extensions/iceberg/overview).

## Coverage limits

The [v3 specification](https://iceberg.apache.org/spec/#version-3-extended-types-and-capabilities)
extends types and metadata beyond these fixtures.

| Capability outside the read tests | This harness's writer route |
|---|---|
| Column defaults | Spark SQL `ADD COLUMN ... DEFAULT` rejected |
| Geometry / geography | Spark SQL type declarations rejected |
| Unknown | Spark SQL type declaration rejected |
| Multi-argument transforms | The attempted `bucket(4, a, b)` declaration rejected; this is one attempted syntax, not proof about every API or transform |
| Nanosecond timestamps | Ordinary `TIMESTAMP_NTZ` / `TIMESTAMP` declarations produce microsecond Iceberg types; an explicit nanosecond schema is not tested |
| Encryption keys | Not attempted; no key-manager setup |

The rejected declarations and ordinary timestamp mappings are re-probed on each
build. Success on a previously blocked route is flagged for adding a reader
test. These observations concern this SQL route and pinned writer, not all
Spark/Iceberg APIs or other engines.

Other useful cases remain outside scope: v2-to-v3 migration with retained position
deletes, equality deletes, schema/partition evolution, concurrent writers,
object storage, and a broad distribution of delete patterns. Three supported
features do not imply complete v3 support.

## Development

```bash
python -m unittest discover -s tests
python tools/check_docs.py
```

GitHub Actions runs those checks and a 200k-row end-to-end build, retaining its
results and logs as an artifact. The documentation check parses Python and shell
code fences; it does not execute snippets or validate SQL.

```
run_all.py            doc/regression checks, build, verify, full logs
bench/checks.py       shared SQL and comparison helpers
bench/build.py        Spark fixtures, writer probes, truth.json
bench/verify.py       DuckDB comparisons, timings, sanitized export
bench/spark_setup.py  Spark session and metadata discovery
tests/test_checks.py  comparison regression tests
tools/check_docs.py   Python/shell fence syntax checks
results/             publication evidence
```

The Medium draft is `ARTICLE.md`, intentionally ignored by Git so the post can
be published separately. No publishing or account credentials are needed to run
this repository.

## License

MIT. See [LICENSE](LICENSE).
