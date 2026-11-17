"""Timeout and bounded-retry paths (every network call has a timeout and a bounded number of retries)."""

from datetime import timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

from taxi import config, ingest, run_job
from taxi.ingest import Month
from taxi.storage import LocalStorage


class Resp:
    def __init__(self, body):
        self.status_code, self.body = 200, body
        self.headers = {"Content-Length": str(len(body))}

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def raise_for_status(self):
        pass

    def iter_content(self, n):
        yield self.body


class ScriptedSession:
    """Raises the given exceptions in order, then serves body; records every timeout argument."""

    def __init__(self, errors, body=b""):
        self.errors, self.body, self.timeouts = list(errors), body, []

    def get(self, url, stream, timeout):
        self.timeouts.append(timeout)
        if self.errors:
            raise self.errors.pop(0)
        return Resp(self.body)


def parquet_body(tmp_path):
    p = tmp_path / "s.parquet"
    pq.write_table(pa.table({"x": [1, 2, 3]}), p)
    return p.read_bytes()


def test_download_sets_connect_and_read_timeout(tmp_path):
    s = ScriptedSession([], b"abc")
    ingest.download("u", tmp_path / "f", s)
    assert s.timeouts == [(config.HTTP_CONNECT_TIMEOUT_S, config.HTTP_READ_TIMEOUT_S)]


def test_download_gives_up_after_bounded_attempts(tmp_path):
    s = ScriptedSession([requests.Timeout("read timed out")] * 10)
    pauses = []
    with pytest.raises(requests.Timeout):
        ingest.ingest_month(Month(2025, 1), LocalStorage(tmp_path / "l"), s, tmp_path / "w", sleep=pauses.append)
    assert len(s.timeouts) == config.ATTEMPTS == 3
    assert pauses == [2, 4]
    assert not (tmp_path / "w" / "raw").exists() or list((tmp_path / "w" / "raw").iterdir()) == []


def test_download_recovers_after_one_timeout(tmp_path):
    s = ScriptedSession([requests.Timeout("read timed out")], parquet_body(tmp_path))
    out = ingest.ingest_month(Month(2025, 1), LocalStorage(tmp_path / "l"), s, tmp_path / "w", sleep=lambda _: None)
    assert out["status"] == "loaded" and out["rows"] == 3
    assert len(s.timeouts) == 2


def test_upload_retried_then_succeeds(tmp_path):
    class Flaky(LocalStorage):
        calls = 0

        def upload(self, local, rel):
            Flaky.calls += 1
            if Flaky.calls < 3:
                raise ConnectionResetError("peer closed")
            super().upload(local, rel)

    st = Flaky(tmp_path / "l")
    s = ScriptedSession([], parquet_body(tmp_path))
    out = ingest.ingest_month(Month(2025, 1), st, s, tmp_path / "w", sleep=lambda _: None)
    assert out["status"] == "loaded" and Flaky.calls == 3


def test_job_wait_limit_cancels_run(monkeypatch):
    cancelled = []

    class Waiter:
        run_id = 42

        def result(self, timeout):
            assert timeout == run_job.RUN_LIMIT
            raise TimeoutError("timed out")

    class Jobs:
        def list(self):
            class S:
                name = "[dev someone] taxi_bronze"

            class J:
                job_id, settings = 7, S()

            return [J()]

        def run_now(self, job_id, job_parameters):
            return Waiter()

        def cancel_run(self, run_id):
            cancelled.append(run_id)

    class W:
        jobs = Jobs()

    monkeypatch.setattr(run_job, "workspace_client", lambda: W())
    assert run_job.main(["taxi_bronze", "2025-01"]) == 1
    assert cancelled == [42]
    assert run_job.RUN_LIMIT <= timedelta(minutes=25)  # inside the Airflow task limit


def test_workspace_client_has_explicit_limits(monkeypatch):
    import databricks.sdk
    import databricks.sdk.config

    seen = {}
    monkeypatch.setattr(databricks.sdk.config, "Config", lambda **kw: seen.update(kw) or kw)
    monkeypatch.setattr(databricks.sdk, "WorkspaceClient", lambda config: ("client", config))
    monkeypatch.setenv("DATABRICKS_HOST", "https://example.cloud.databricks.com?o=1")
    assert config.workspace_client()[0] == "client"
    assert seen == {
        "host": "https://example.cloud.databricks.com",
        "http_timeout_seconds": config.HTTP_READ_TIMEOUT_S,
        "retry_timeout_seconds": config.SDK_RETRY_BUDGET_S,
    }


# ---------- review ingest retries: 5xx, broken streams, missing Content-Length, old 403 ----------


class StatusResp(Resp):
    def __init__(self, status, body=b"", headers=None):
        super().__init__(body)
        self.status_code = status
        if headers is not None:
            self.headers = headers


class SeqSession:
    def __init__(self, responses):
        self.responses, self.calls = list(responses), 0

    def get(self, url, stream, timeout):
        self.calls += 1
        r = self.responses.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


def test_5xx_then_success_is_retried(tmp_path):
    s = SeqSession([StatusResp(503), StatusResp(200, parquet_body(tmp_path))])
    out = ingest.ingest_month(Month(2025, 1), LocalStorage(tmp_path / "l"), s, tmp_path / "w", sleep=lambda _: None)
    assert out["status"] == "loaded" and s.calls == 2


def test_broken_stream_is_retried(tmp_path):
    s = SeqSession([requests.exceptions.ChunkedEncodingError("cut"), StatusResp(200, parquet_body(tmp_path))])
    out = ingest.ingest_month(Month(2025, 1), LocalStorage(tmp_path / "l"), s, tmp_path / "w", sleep=lambda _: None)
    assert out["status"] == "loaded" and s.calls == 2


def test_missing_content_length_is_a_size_mismatch(tmp_path):
    with pytest.raises(ingest.SizeMismatch):
        ingest.download("u", tmp_path / "f", SeqSession([StatusResp(200, b"abc", headers={})]))


def test_recent_403_is_not_published_but_an_old_403_fails(tmp_path):
    from datetime import date

    recent = ingest.ingest_month(
        Month(2026, 10), LocalStorage(tmp_path / "l"), SeqSession([StatusResp(403)]), tmp_path / "w", today=date(2026, 11, 17)
    )
    assert recent["status"] == "not_published"
    with pytest.raises(ingest.SourceMissing):
        ingest.ingest_month(
            Month(2024, 1), LocalStorage(tmp_path / "l"), SeqSession([StatusResp(404)]), tmp_path / "w", today=date(2026, 11, 17)
        )
