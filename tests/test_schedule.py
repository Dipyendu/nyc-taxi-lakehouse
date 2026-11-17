from datetime import datetime

import pytest

from taxi.schedule import LAG_MONTHS, target_month


def test_scheduled_run_loads_four_months_back():
    assert LAG_MONTHS == 4
    assert target_month(None, "scheduled", datetime(2026, 12, 1), datetime(2026, 12, 1)) == "2026-08"


def test_scheduled_run_wraps_the_year():
    assert target_month(None, "scheduled", datetime(2027, 2, 1), datetime(2027, 2, 1)) == "2026-10"
    assert target_month(None, "scheduled", datetime(2027, 4, 1), datetime(2027, 4, 1)) == "2026-12"


def test_scheduled_run_of_1_nov_2026_loads_2026_07():
    # The one scheduled run that exists in this project (scheduled__2026-11-01).
    assert target_month(None, "scheduled", datetime(2026, 11, 1), datetime(2026, 11, 1)) == "2026-07"


def test_backfill_loads_month_of_its_logical_date():
    assert target_month(None, "backfill", datetime(2024, 3, 1), datetime(2024, 3, 1)) == "2024-03"


def test_param_month_wins_for_any_run_type():
    for kind in ("manual", "scheduled", "backfill"):
        assert target_month("2025-02", kind, datetime(2026, 11, 17), datetime(2026, 11, 17)) == "2025-02"


def test_manual_run_without_month_or_logical_date_uses_run_after_minus_lag():
    assert target_month(None, "manual", None, datetime(2026, 11, 17, 9, 30)) == "2026-07"


def test_enum_like_run_type_is_accepted():
    class RunType:
        value = "backfill"

    assert target_month(None, RunType(), datetime(2025, 6, 1), None) == "2025-06"


@pytest.mark.parametrize("bad", ["2025-13", "2025-1", "25-01", "2025-01'; DROP TABLE x; --"])
def test_param_month_is_validated(bad):
    with pytest.raises(ValueError):
        target_month(bad, "manual", None, datetime(2026, 11, 17))


def test_no_date_at_all_is_an_error():
    with pytest.raises(ValueError):
        target_month(None, "manual", None, None)
