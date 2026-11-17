"""One NYC yellow-taxi month: ingest -> bronze -> silver -> dbt build -> reconciliation.

Which month a run loads is decided by taxi.schedule.target_month:
- the `month` param, when given;
- for a backfill run, the month of its logical date (the run dated 2024-03-01 loads 2024-03);
- for a scheduled run, or a manual run without `month`, the month four months before its date
  (the run on 1 Dec 2026 loads 2026-08), because TLC files appear up to 83 days after their month.
An unpublished month makes `ingest` exit 99, which BashOperator turns into a skip, so the rest of the
run is skipped rather than failed. Each task is idempotent, so a failed task is retried twice.

Backfill example:
  airflow backfill create --dag-id taxi_monthly --from-date 2024-01-01 --to-date 2024-12-01
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

from taxi.schedule import target_month  # standard library only; /opt/taxi/src is on PYTHONPATH

MONTH = "{{ target_month(params.month, dag_run.run_type, logical_date, dag_run.run_after) }}"
PY = "/opt/taxi-venv/bin/python"
DBT = "/opt/taxi-venv/bin/dbt"
LEDGER = "/opt/taxi/scripts/ledger.sh"


def ledgered(what: str, command: str) -> str:
    return f'{LEDGER} "airflow {what} {MONTH}" "1 month" {command}'


with DAG(
    dag_id="taxi_monthly",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    # Wall-clock limits: one month takes about 3 to 7 minutes (docs/evidence/a4-airflow-clean-start-2025-02.txt).
    dagrun_timeout=timedelta(minutes=60),
    default_args={"execution_timeout": timedelta(minutes=25), "retries": 2, "retry_delay": timedelta(minutes=2)},
    params={"month": Param(None, type=["null", "string"], pattern=r"^\d{4}-(0[1-9]|1[0-2])$")},
    user_defined_macros={"target_month": target_month},
    tags=["nyc-taxi"],
) as dag:
    ingest = BashOperator(
        task_id="ingest", bash_command="cd /opt/taxi && " + ledgered("ingest", f"{PY} -m taxi.ingest {MONTH}")
    )
    bronze = BashOperator(task_id="bronze", bash_command=ledgered("bronze", f"{PY} -m taxi.run_job taxi_bronze {MONTH}"))
    silver = BashOperator(task_id="silver", bash_command=ledgered("silver", f"{PY} -m taxi.run_job taxi_silver {MONTH}"))
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="cd /opt/taxi/dbt && "
        + ledgered("dbt build", f"{DBT} build --profiles-dir . --vars \"{{month: '{MONTH}'}}\" --exclude reconcile_months"),
    )
    reconcile = BashOperator(
        task_id="reconcile",
        bash_command="cd /opt/taxi/dbt && "
        + ledgered("dbt reconcile", f"{DBT} test --profiles-dir . --select reconcile_months"),
    )

    ingest >> bronze >> silver >> dbt_build >> reconcile
