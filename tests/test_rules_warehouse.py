"""Run the silver CASE expression on handmade rows in the SQL warehouse (Databricks SQL, the same
SQL engine family as the Spark job). Opt-in: TAXI_WAREHOUSE_TESTS=1 and credentials in the environment.
"""

import os

import pytest

pytestmark = pytest.mark.skipif(os.environ.get("TAXI_WAREHOUSE_TESTS") != "1", reason="needs the SQL warehouse")

# id, file_month, pickup_ts, dropoff_ts, trip_distance, fare_amount, total_amount, expected rule
ROWS = [
    (1, "2025-01", "2025-01-05 10:00:00", "2025-01-05 10:20:00", 3.0, 15.0, 20.0, None),
    (2, "2025-01", None, "2025-01-05 10:20:00", 3.0, 15.0, 20.0, "missing_timestamp"),
    (3, "2025-01", "2024-12-31 23:50:00", "2025-01-01 00:10:00", 3.0, 15.0, 20.0, "pickup_outside_file_month"),
    (4, "2025-01", "2025-01-05 10:20:00", "2025-01-05 10:00:00", 3.0, 15.0, 20.0, "dropoff_before_pickup"),
    (5, "2025-01", "2025-01-05 10:00:00", "2025-01-06 10:00:00", 3.0, 15.0, 20.0, None),  # exactly 24 h
    (6, "2025-01", "2025-01-05 10:00:00", "2025-01-06 10:00:01", 3.0, 15.0, 20.0, "duration_over_24h"),
    (7, "2025-01", "2025-01-05 10:00:00", "2025-01-05 10:20:00", -0.1, 15.0, 20.0, "negative_trip_distance"),
    (8, "2025-01", "2025-01-05 10:00:00", "2025-01-05 10:20:00", 500.0, 15.0, 20.0, None),  # exactly 500 mi
    (9, "2025-01", "2025-01-05 10:00:00", "2025-01-05 10:20:00", 500.01, 15.0, 20.0, "trip_distance_over_500mi"),
    (10, "2025-01", "2025-01-05 10:00:00", "2025-01-05 10:20:00", 3.0, -5.0, -6.0, "negative_fare_or_total"),
    (11, "2025-01", "2025-01-05 10:00:00", "2025-01-05 10:20:00", 3.0, None, 20.0, None),  # NULL fare falls through
    (12, "2025-01", "2025-01-05 10:20:00", "2025-01-05 10:00:00", 3.0, -5.0, -6.0, "dropoff_before_pickup"),  # 2 rules
    (13, "2025-01", "2025-01-05 10:00:00", "2025-01-05 10:20:00", None, None, None, None),  # all NULL measures
    (14, "2025-01", "2025-01-05 10:00:00", "2025-01-05 10:20:00", 3.0, 15.0, -0.5, "negative_fare_or_total"),
]


def lit(v):
    if v is None:
        return "NULL"
    if isinstance(v, str) and v[:2] == "20" and " " in v:
        return f"TIMESTAMP_NTZ '{v}'"
    if isinstance(v, str):
        return f"'{v}'"
    return f"CAST({v} AS DOUBLE)" if isinstance(v, float) else str(v)


def test_rules_on_handmade_rows(silver):
    from taxi.config import warehouse_id, workspace_client

    values = ", ".join(
        "(" + ", ".join(lit(v) for v in r[:7]).replace("TIMESTAMP_NTZ 'None'", "NULL") + ")" for r in ROWS
    )
    # Typed NULLs: cast every column so a NULL row cannot change the inferred type.
    sql = (
        "SELECT id, " + silver.rule_case_sql() + " AS rule FROM ("
        "SELECT CAST(id AS INT) id, CAST(file_month AS STRING) file_month, CAST(pickup_ts AS TIMESTAMP_NTZ) pickup_ts, "
        "CAST(dropoff_ts AS TIMESTAMP_NTZ) dropoff_ts, CAST(trip_distance AS DOUBLE) trip_distance, "
        "CAST(fare_amount AS DOUBLE) fare_amount, CAST(total_amount AS DOUBLE) total_amount "
        f"FROM VALUES {values} AS t(id, file_month, pickup_ts, dropoff_ts, trip_distance, fare_amount, total_amount)"
        ") ORDER BY id"
    )
    w = workspace_client()
    r = w.statement_execution.execute_statement(warehouse_id=warehouse_id(), statement=sql, wait_timeout="50s")
    assert r.status.state.value == "SUCCEEDED", r.status.error
    got = {int(i): rule for i, rule in r.result.data_array}
    expected = {r[0]: r[7] for r in ROWS}
    print("\nid | expected | got")
    for i in sorted(expected):
        print(f"{i} | {expected[i]} | {got[i]}")
    assert got == expected
