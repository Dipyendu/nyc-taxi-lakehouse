"""Which month an Airflow run of taxi_monthly loads. Standard library only, so the DAG can import it.

- A run with the `month` parameter loads that month.
- A backfill run loads the month of its logical date (the run dated 2024-03-01 loads 2024-03).
- A scheduled run, or a manual run without `month`, loads the month LAG_MONTHS before its date.
  The TLC files appeared 18 to 83 days after their month ended (HTTP Last-Modified of 2024-01..2026-09,
  docs/evidence/a8/b1-publication-lag.txt). A run on the 1st of a month comes 89 to 92 days after the end of
  the month four back; all 33 files were out by then, the closest with 7 days to spare. Two or three months
  back would have missed some. A file later than about 89 days is never loaded by the schedule.
"""

from __future__ import annotations

import re
from datetime import datetime

LAG_MONTHS = 4
_MONTH = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def months_before(when: datetime, n: int) -> str:
    total = when.year * 12 + (when.month - 1) - n
    return f"{total // 12:04d}-{total % 12 + 1:02d}"


def target_month(param_month: str | None, run_type, logical_date: datetime | None, run_after: datetime | None) -> str:
    """Month (YYYY-MM) for one DAG run. run_type is Airflow's DagRunType or its string value."""
    if param_month:
        if not _MONTH.match(param_month):
            raise ValueError(f"month must be YYYY-MM, got {param_month!r}")
        return param_month
    kind = getattr(run_type, "value", run_type)
    when = logical_date or run_after  # a manual run in Airflow 3 may have no logical date
    if when is None:
        raise ValueError("run has neither a logical date nor a run_after time")
    if kind == "backfill":
        return months_before(when, 0)
    return months_before(when, LAG_MONTHS)
