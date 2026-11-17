"""Run a deployed bundle job for one month and wait for it, within a wall-clock limit.

Usage: python -m taxi.run_job taxi_bronze 2025-01
Bundle development mode prefixes job names ("[dev user] taxi_bronze"), so the match is on the name suffix.
If the run is not finished within RUN_LIMIT, it is cancelled and the command fails.
"""

import sys
from datetime import datetime, timedelta

from databricks.sdk.errors import OperationFailed

from taxi.config import workspace_client

RUN_LIMIT = timedelta(minutes=20)  # a month's job takes about 30 s (docs/run-ledger.md)


def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def find_job_id(w, name: str) -> int:
    ids = [j.job_id for j in w.jobs.list() if j.settings and j.settings.name.endswith(name)]
    if len(ids) != 1:
        raise SystemExit(f"expected one job named *{name}, found {len(ids)}; run `make deploy`")
    return ids[0]


def main(argv: list[str]) -> int:
    name, month = argv
    w = workspace_client()
    job_id = find_job_id(w, name)
    waiter = w.jobs.run_now(job_id=job_id, job_parameters={"month": month})
    log(f"{name} month={month} job_id={job_id} run_id={waiter.run_id} started; limit {RUN_LIMIT}")
    try:
        run = waiter.result(timeout=RUN_LIMIT)
    except TimeoutError:
        w.jobs.cancel_run(run_id=waiter.run_id)
        log(f"{name} month={month} run_id={waiter.run_id} exceeded {RUN_LIMIT}; cancel requested")
        return 1
    except OperationFailed as e:  # the run ended FAILED, CANCELED or similar
        log(f"{name} month={month} run_id={waiter.run_id} failed: {type(e).__name__}: {str(e)[:300]}")
        return 1
    state = run.state.result_state.value if run.state and run.state.result_state else "UNKNOWN"
    log(f"{name} month={month} job_id={job_id} run_id={run.run_id} result={state}")  # no URL: it names the workspace
    return 0 if state == "SUCCESS" else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
