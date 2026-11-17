"""Row count, content checksum, Delta version and latest load time for one month in every layer.

Usage: uv run python scripts/idempotency_snapshot.py YYYY-MM
The checksum covers every column except _load_ts, which changes on each load by design; the Delta
version and max(_load_ts) show whether a reload actually wrote, so an empty reload cannot pass.
"""

import re
import sys

from taxi.config import CATALOG, warehouse_id, workspace_client

TABLES = [
    ("bronze", "taxi_bronze.yellow_trips"),
    ("silver", "taxi_silver.yellow_trips"),
    ("quarantine", "taxi_silver.yellow_trips_quarantine"),
    ("fact", "taxi_gold.fct_trips"),
]


def main(month: str) -> int:
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", month):
        raise SystemExit("month must be YYYY-MM")
    w, wid = workspace_client(), warehouse_id()

    def query(sql):
        r = w.statement_execution.execute_statement(warehouse_id=wid, statement=sql, wait_timeout="50s")
        if r.status.state.value != "SUCCEEDED":
            raise SystemExit(f"{r.status.state.value}: {r.status.error.message if r.status.error else ''}")
        return r.result.data_array or []

    print("layer | rows | checksum | delta_version | max_load_ts")
    for layer, name in TABLES:
        full = f"{CATALOG}.{name}"
        cols = [c.name for c in w.tables.get(full).columns if c.name != "_load_ts"]
        has_load_ts = any(c.name == "_load_ts" for c in w.tables.get(full).columns)
        col_sql = ", ".join(f"`{c}`" for c in cols)
        load = "max(_load_ts)" if has_load_ts else "NULL"
        rows, checksum, max_load = query(
            f"SELECT count(*), sum(cast(xxhash64({col_sql}) AS DECIMAL(38,0))), {load} FROM {full} WHERE file_month = '{month}'"
        )[0]
        version = query(f"DESCRIBE HISTORY {full} LIMIT 1")[0][0]
        print(f"{layer} | {rows} | {checksum} | {version} | {max_load}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
