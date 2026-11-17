"""Run SQL statements on the project's SQL warehouse and print the results (used for evidence).

Usage: uv run python scripts/sql.py "SELECT ..." ["SELECT ..." ...]
"""

import sys

from taxi.config import warehouse_id, workspace_client


def main(statements: list[str]) -> int:
    w = workspace_client()
    rc = 0
    for st in statements:
        r = w.statement_execution.execute_statement(warehouse_id=warehouse_id(), statement=st, wait_timeout="50s")
        print(f"> {st}")
        if r.status.state.value != "SUCCEEDED":
            print(f"  {r.status.state.value}: {r.status.error.message if r.status.error else ''}")
            rc = 1
            continue
        cols = [c.name for c in r.manifest.schema.columns] if r.manifest and r.manifest.schema else []
        print("  " + " | ".join(cols))
        for row in (r.result.data_array or []) if r.result else []:
            print("  " + " | ".join("NULL" if v is None else str(v) for v in row))
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
