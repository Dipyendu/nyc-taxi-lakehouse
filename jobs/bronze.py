"""Bronze: load one month's landed Parquet file into a Delta table, replacing exactly that month.

Runs on Databricks serverless as a spark_python_task. Self-contained: no imports from src/taxi.
Args: --catalog CAT --landing-root /Volumes/... --month YYYY-MM
"""

import argparse
import json
import re

_MONTH = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def valid_month(month: str) -> str:
    """The month goes into SQL text and a path, so only YYYY-MM is accepted."""
    if not _MONTH.match(month):
        raise SystemExit(f"--month must be YYYY-MM, got {month!r}")
    return month


def landing_file(landing_root: str, month: str) -> str:
    """Path of one month's landed file; the same layout as taxi.ingest.landing_path."""
    y, m = valid_month(month).split("-")
    return f"{landing_root.rstrip('/')}/yellow/year={y}/month={m}/yellow_tripdata_{month}.parquet"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--landing-root", required=True)
    p.add_argument("--month", required=True)
    a = p.parse_args()
    path = landing_file(a.landing_root, a.month)
    root = a.landing_root.rstrip("/")
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {a.catalog}.taxi_bronze")
    trips = f"{a.catalog}.taxi_bronze.yellow_trips"
    manifest = f"{a.catalog}.taxi_bronze.ingest_manifest"
    where = f"file_month = '{a.month}'"

    df = (
        spark.read.parquet(path)
        .select(
            "*",
            F.col("_metadata.file_name").alias("_source_file"),
            F.col("_metadata.row_index").alias("_source_row"),
        )
        .withColumn("_load_ts", F.current_timestamp())
        .withColumn("file_month", F.lit(a.month))
    )
    # Overwrite only this month's partition; new source columns are added to the table, never dropped.
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", where)
        .option("mergeSchema", "true")
        .partitionBy("file_month")
        .saveAsTable(trips)
    )

    man = (
        spark.read.option("multiLine", "true")
        .json(f"{root}/_manifest/yellow/{a.month}.json")
        .select(
            F.col("month").alias("file_month"),
            "file",
            "bytes",
            "rows",
            "schema_hash",
            F.to_json("schema").alias("schema_json"),
            F.col("uploaded_at").cast("timestamp").alias("uploaded_at"),
        )
    )
    man.write.format("delta").mode("overwrite").option("replaceWhere", where).saveAsTable(manifest)

    bronze_rows = spark.table(trips).where(where).count()
    manifest_rows = man.first()["rows"]
    print(json.dumps({"month": a.month, "bronze_rows": bronze_rows, "manifest_rows": manifest_rows}))
    if bronze_rows != manifest_rows:
        raise SystemExit(f"row mismatch: bronze {bronze_rows} != manifest {manifest_rows}")


if __name__ == "__main__":
    main()
