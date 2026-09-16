"""Spark + Iceberg session and shared paths for the v3 bench."""
import glob
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
WAREHOUSE = os.path.join(ROOT, "warehouse")
TRUTH_PATH = os.path.join(ROOT, "truth.json")

ICEBERG_VERSION = "1.11.0"
RUNTIME = f"org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:{ICEBERG_VERSION}"

# Spark needs a JDK; honour an existing JAVA_HOME, else try the Homebrew default.
if not os.environ.get("JAVA_HOME"):
    for candidate in ("/opt/homebrew/opt/openjdk@21", "/usr/local/opt/openjdk@21"):
        if os.path.isdir(candidate):
            os.environ["JAVA_HOME"] = candidate
            break
if os.environ.get("JAVA_HOME"):
    os.environ["PATH"] = f"{os.environ['JAVA_HOME']}/bin:" + os.environ["PATH"]


def get_spark(app="iceberg-v3-bench"):
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(app)
        .config("spark.jars.packages", RUNTIME)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.demo.type", "hadoop")
        .config("spark.sql.catalog.demo.warehouse", WAREHOUSE)
        .config("spark.sql.defaultCatalog", "demo")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .master("local[2]")
        .getOrCreate()
    )


def metadata_json(table):
    """Newest metadata.json for a `namespace.table`, for handing to DuckDB.

    DuckDB's iceberg_scan() reads a metadata file directly, so this resolves the
    highest-numbered vN.metadata.json rather than relying on version-hint.text.
    """
    d = os.path.join(WAREHOUSE, *table.split("."), "metadata")
    files = glob.glob(os.path.join(d, "v*.metadata.json"))
    if not files:
        raise FileNotFoundError(f"no metadata for {table} in {d} -- run build.py first")
    return max(files, key=lambda p: int(re.match(r"v(\d+)", os.path.basename(p)).group(1)))
