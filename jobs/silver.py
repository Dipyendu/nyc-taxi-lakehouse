"""Silver: typed, cleaned trips for one month; rule-breaking rows go to quarantine with the rule name.

Each bronze row lands in exactly one of silver or quarantine, so bronze = silver + quarantine.
Runs on Databricks serverless as a spark_python_task. Self-contained: no imports from src/taxi.
Args: --catalog CAT --month YYYY-MM

RULES and rule_case_sql() are the one definition of the cleaning rules. The job applies
rule_case_sql() as a Spark SQL expression; tests/test_rules.py checks it without Spark and
tests/test_rules_warehouse.py runs it on handmade rows in the SQL warehouse.
"""

import argparse
import json
import re

# (output name, bronze source name, type). Columns absent in older months read as NULL from the merged table.
COLUMNS = [
    ("vendor_id", "VendorID", "int"),
    ("pickup_ts", "tpep_pickup_datetime", "timestamp_ntz"),
    ("dropoff_ts", "tpep_dropoff_datetime", "timestamp_ntz"),
    ("passenger_count", "passenger_count", "int"),
    ("trip_distance", "trip_distance", "double"),
    ("ratecode_id", "RatecodeID", "int"),
    ("store_and_fwd_flag", "store_and_fwd_flag", "string"),
    ("pu_location_id", "PULocationID", "int"),
    ("do_location_id", "DOLocationID", "int"),
    ("payment_type", "payment_type", "int"),
    ("fare_amount", "fare_amount", "double"),
    ("extra", "extra", "double"),
    ("mta_tax", "mta_tax", "double"),
    ("tip_amount", "tip_amount", "double"),
    ("tolls_amount", "tolls_amount", "double"),
    ("improvement_surcharge", "improvement_surcharge", "double"),
    ("total_amount", "total_amount", "double"),
    ("congestion_surcharge", "congestion_surcharge", "double"),
    ("airport_fee", "Airport_fee", "double"),
    ("cbd_congestion_fee", "cbd_congestion_fee", "double"),
    ("request_source", "request_source", "string"),
]

# First matching rule wins, so each row carries exactly one rule name. Order: most fundamental first.
RULES = [
    ("missing_timestamp", "pickup_ts IS NULL OR dropoff_ts IS NULL"),
    ("pickup_outside_file_month", "date_format(pickup_ts, 'yyyy-MM') <> file_month"),
    ("dropoff_before_pickup", "dropoff_ts < pickup_ts"),
    ("duration_over_24h", "dropoff_ts > pickup_ts + INTERVAL 24 HOURS"),
    ("negative_trip_distance", "trip_distance < 0"),
    ("trip_distance_over_500mi", "trip_distance > 500"),
    ("negative_fare_or_total", "fare_amount < 0 OR total_amount < 0"),
]

_MONTH = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def valid_month(month: str) -> str:
    """The month goes into SQL text, so only YYYY-MM is accepted."""
    if not _MONTH.match(month):
        raise SystemExit(f"--month must be YYYY-MM, got {month!r}")
    return month


def rule_case_sql() -> str:
    """One CASE expression: the first rule whose condition is TRUE names the row.

    A condition that is NULL (for example fare_amount < 0 when fare_amount is NULL) is not TRUE,
    so evaluation falls through to the next rule, and a row no rule matches gets NULL: it stays in silver.
    """
    whens = " ".join(f"WHEN ({cond}) THEN '{name}'" for name, cond in RULES)
    return f"CASE {whens} ELSE NULL END"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--month", required=True)
    a = p.parse_args()
    valid_month(a.month)
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {a.catalog}.taxi_silver")
    where = f"file_month = '{a.month}'"

    bronze = spark.table(f"{a.catalog}.taxi_bronze.yellow_trips").where(where)
    have = set(bronze.columns)
    cols = [
        (F.col(src) if src in have else F.lit(None)).cast(typ).alias(out) for out, src, typ in COLUMNS
    ]
    typed = bronze.select(
        F.concat_ws("-", "file_month", F.col("_source_row").cast("string")).alias("trip_id"),
        "file_month",
        *cols,
        "_source_file",
        "_source_row",
        "_load_ts",
    )
    flagged = typed.withColumn("_rule", F.expr(rule_case_sql()))

    def write(df, table):
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", where)
            .option("mergeSchema", "true")
            .partitionBy("file_month")
            .saveAsTable(f"{a.catalog}.taxi_silver.{table}")
        )

    write(flagged.where("_rule IS NULL").drop("_rule"), "yellow_trips")
    write(flagged.where("_rule IS NOT NULL").withColumnRenamed("_rule", "rule"), "yellow_trips_quarantine")

    s = spark.table(f"{a.catalog}.taxi_silver.yellow_trips").where(where).count()
    q = spark.table(f"{a.catalog}.taxi_silver.yellow_trips_quarantine").where(where)
    per_rule = {r["rule"]: r["n"] for r in q.groupBy("rule").agg(F.count("*").alias("n")).collect()}
    b = bronze.count()
    print(json.dumps({"month": a.month, "bronze": b, "silver": s, "quarantine": per_rule}))
    if b != s + sum(per_rule.values()):
        raise SystemExit("reconciliation failed: bronze != silver + quarantine")


if __name__ == "__main__":
    main()
