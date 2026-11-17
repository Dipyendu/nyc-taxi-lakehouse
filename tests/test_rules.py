"""Silver cleaning rules and bronze helpers, checked without Spark."""

import re

import pytest


def test_rule_names_are_unique_and_in_the_documented_order(silver):
    names = [n for n, _ in silver.RULES]
    assert names == [
        "missing_timestamp",
        "pickup_outside_file_month",
        "dropoff_before_pickup",
        "duration_over_24h",
        "negative_trip_distance",
        "trip_distance_over_500mi",
        "negative_fare_or_total",
    ]
    assert len(set(names)) == len(names)


def test_case_lists_every_rule_once_in_order_and_defaults_to_null(silver):
    sql = silver.rule_case_sql()
    assert sql.startswith("CASE WHEN ") and sql.endswith(" ELSE NULL END")
    found = re.findall(r"THEN '([a-z0-9_]+)'", sql)
    assert found == [n for n, _ in silver.RULES]  # first match wins = this order


def test_each_condition_is_parenthesised_so_or_cannot_leak_into_the_next_when(silver):
    sql = silver.rule_case_sql()
    for _, cond in silver.RULES:
        assert f"WHEN ({cond}) THEN" in sql


def test_rule_columns_exist_in_silver_output(silver):
    out_cols = {out for out, _, _ in silver.COLUMNS} | {"file_month"}
    used = set(re.findall(r"\b([a-z_]+_ts|trip_distance|fare_amount|total_amount|file_month)\b", silver.rule_case_sql()))
    assert used <= out_cols


@pytest.mark.parametrize("bad", ["2025-1", "2025-13", "2025-01'--", ""])
def test_jobs_reject_a_bad_month(silver, bronze, bad):
    for job in (silver, bronze):
        with pytest.raises(SystemExit):
            job.valid_month(bad)


def test_bronze_path_matches_ingest_layout(bronze):
    from taxi.ingest import Month, landing_path

    assert bronze.landing_file("/Volumes/c/s/v/", "2025-01") == "/Volumes/c/s/v/" + landing_path(Month(2025, 1))
