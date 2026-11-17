"""Project configuration. LANDING_ROOT is the only place that names the landing location."""

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

SOURCE_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

# Landing location for raw files and manifests. A /Volumes/... path means a Unity Catalog
# volume reached through the Files API; anything else is a local directory (tests, offline runs).
CATALOG = os.environ.get("TAXI_CATALOG", "analytics")
LANDING_ROOT = os.environ.get("TAXI_LANDING_ROOT", f"/Volumes/{CATALOG}/taxi_landing/raw")


def warehouse_id() -> str:
    """The SQL warehouse id, from TAXI_WAREHOUSE_HTTP_PATH (set in local.env; dbt reads the same variable)."""
    import re

    path = os.environ.get("TAXI_WAREHOUSE_HTTP_PATH", "")
    m = re.fullmatch(r"/sql/1\.0/warehouses/([0-9a-f]+)", path)
    if not m:
        raise SystemExit("set TAXI_WAREHOUSE_HTTP_PATH=/sql/1.0/warehouses/<id> (copy local.env.example to local.env)")
    return m.group(1)

# Local cache for downloaded files (gitignored).
LOCAL_DATA_DIR = Path(os.environ.get("TAXI_DATA_DIR", REPO_ROOT / "data"))


# Network limits: every call has a timeout and a bounded number of retries.
HTTP_CONNECT_TIMEOUT_S = 10
HTTP_READ_TIMEOUT_S = 120  # longest wait for any single socket read or write
ATTEMPTS = 3  # tries per download or upload, with 2 s and 4 s pauses between them
SDK_RETRY_BUDGET_S = 300  # the Databricks SDK stops retrying a call after this long


def workspace_client():
    """Databricks client with explicit per-request timeout and a bounded retry window."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.config import Config

    cfg = Config(
        host=databricks_host(),
        http_timeout_seconds=HTTP_READ_TIMEOUT_S,
        retry_timeout_seconds=SDK_RETRY_BUDGET_S,
    )
    return WorkspaceClient(config=cfg)


def databricks_host() -> str | None:
    """DATABRICKS_HOST without a browser query string such as '?o=<workspace id>' (the SDK rejects it)."""
    host = os.environ.get("DATABRICKS_HOST")
    return host.split("?", 1)[0].rstrip("/") if host else None
