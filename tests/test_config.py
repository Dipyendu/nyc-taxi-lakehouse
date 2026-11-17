import pytest

from taxi import config


@pytest.mark.parametrize(
    "raw,clean",
    [
        ("https://dbc-x.cloud.databricks.com?o=123", "https://dbc-x.cloud.databricks.com"),
        ("https://dbc-x.cloud.databricks.com/?o=123", "https://dbc-x.cloud.databricks.com"),
        ("https://dbc-x.cloud.databricks.com/", "https://dbc-x.cloud.databricks.com"),
        ("https://dbc-x.cloud.databricks.com", "https://dbc-x.cloud.databricks.com"),
    ],
)
def test_databricks_host_strips_query_and_slash(monkeypatch, raw, clean):
    monkeypatch.setenv("DATABRICKS_HOST", raw)
    assert config.databricks_host() == clean


def test_databricks_host_unset(monkeypatch):
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    assert config.databricks_host() is None


def test_warehouse_id_comes_from_the_environment(monkeypatch):
    monkeypatch.setenv("TAXI_WAREHOUSE_HTTP_PATH", "/sql/1.0/warehouses/0123abcd")
    assert config.warehouse_id() == "0123abcd"


def test_missing_warehouse_path_is_a_clear_error(monkeypatch):
    monkeypatch.delenv("TAXI_WAREHOUSE_HTTP_PATH", raising=False)
    with pytest.raises(SystemExit):
        config.warehouse_id()
