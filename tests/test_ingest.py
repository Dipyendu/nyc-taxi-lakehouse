import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from taxi import ingest
from taxi.ingest import Month, NotPublished, SizeMismatch, month_range
from taxi.storage import LocalStorage

# ---------- month arithmetic ----------

def test_parse_and_str_roundtrip():
    assert str(Month.parse("2025-01")) == "2025-01"


@pytest.mark.parametrize("bad", ["2025-13", "2025-00", "25-01", "2025"])
def test_parse_rejects_bad_month(bad):
    with pytest.raises(ValueError):
        Month.parse(bad)


def test_next_wraps_year():
    assert Month(2024, 12).next() == Month(2025, 1)


def test_month_range_inclusive_across_years():
    r = month_range(Month(2024, 11), Month(2025, 2))
    assert [str(m) for m in r] == ["2024-11", "2024-12", "2025-01", "2025-02"]


def test_full_project_range_is_33_months():
    assert len(month_range(Month(2024, 1), Month(2026, 9))) == 33


def test_month_range_rejects_reversed():
    with pytest.raises(ValueError):
        month_range(Month(2025, 2), Month(2025, 1))


def test_landing_path_is_partitioned():
    assert ingest.landing_path(Month(2025, 1)) == "yellow/year=2025/month=01/yellow_tripdata_2025-01.parquet"


# ---------- fake HTTP ----------

class FakeResponse:
    def __init__(self, status, body=b"", content_length=None):
        self.status_code = status
        self.body = body
        self.headers = {"Content-Length": str(len(body) if content_length is None else content_length)}

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def iter_content(self, n):
        for i in range(0, len(self.body), n):
            yield self.body[i : i + n]


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = 0

    def get(self, url, stream, timeout):
        self.calls += 1
        return self.response


def parquet_bytes(tmp_path, rows=5):
    p = tmp_path / "src.parquet"
    pq.write_table(pa.table({"VendorID": pa.array(range(rows), pa.int32()), "fare_amount": [1.0] * rows}), p)
    return p.read_bytes()


# ---------- failure paths ----------

def test_403_is_not_published_and_leaves_no_file(tmp_path):
    with pytest.raises(NotPublished):
        ingest.download("u", tmp_path / "f.parquet", FakeSession(FakeResponse(403)))
    assert list(tmp_path.iterdir()) == []


def test_size_mismatch_raises_and_leaves_no_file(tmp_path):
    s = FakeSession(FakeResponse(200, b"abc", content_length=10))
    with pytest.raises(SizeMismatch):
        ingest.download("u", tmp_path / "f.parquet", s)
    assert list(tmp_path.iterdir()) == []


def test_ingest_month_reports_not_published(tmp_path):
    st = LocalStorage(tmp_path / "landing")
    from datetime import date

    out = ingest.ingest_month(Month(2026, 10), st, FakeSession(FakeResponse(403)), tmp_path / "work", today=date(2026, 11, 17))
    assert out["status"] == "not_published"
    assert st.read_bytes(ingest.manifest_path(Month(2026, 10))) is None


# ---------- manifest and skip-if-present ----------

def test_ingest_writes_file_and_manifest(tmp_path):
    body = parquet_bytes(tmp_path, rows=7)
    st = LocalStorage(tmp_path / "landing")
    m = Month(2025, 1)
    out = ingest.ingest_month(m, st, FakeSession(FakeResponse(200, body)), tmp_path / "work")
    assert out == {"month": "2025-01", "status": "loaded", "bytes": len(body), "rows": 7}
    rec = json.loads(st.read_bytes(ingest.manifest_path(m)))
    assert rec["rows"] == 7 and rec["bytes"] == len(body) == st.size(ingest.landing_path(m))
    assert rec["schema"] == [["VendorID", "int32"], ["fare_amount", "double"]]
    assert len(rec["schema_hash"]) == 16


def test_second_call_skips_download_and_upload(tmp_path):
    body = parquet_bytes(tmp_path)
    st = LocalStorage(tmp_path / "landing")
    s = FakeSession(FakeResponse(200, body))
    ingest.ingest_month(Month(2025, 1), st, s, tmp_path / "work")
    mtime = (tmp_path / "landing" / ingest.landing_path(Month(2025, 1))).stat().st_mtime_ns
    assert ingest.ingest_month(Month(2025, 1), st, s, tmp_path / "work")["status"] == "skipped"
    assert s.calls == 1
    assert (tmp_path / "landing" / ingest.landing_path(Month(2025, 1))).stat().st_mtime_ns == mtime


def test_force_downloads_again(tmp_path):
    body = parquet_bytes(tmp_path)
    st = LocalStorage(tmp_path / "landing")
    s = FakeSession(FakeResponse(200, body))
    ingest.ingest_month(Month(2025, 1), st, s, tmp_path / "work")
    assert ingest.ingest_month(Month(2025, 1), st, s, tmp_path / "work", force=True)["status"] == "loaded"
    assert s.calls == 2


def test_manifest_without_matching_file_is_not_loaded(tmp_path):
    body = parquet_bytes(tmp_path)
    st = LocalStorage(tmp_path / "landing")
    m = Month(2025, 1)
    ingest.ingest_month(m, st, FakeSession(FakeResponse(200, body)), tmp_path / "work")
    (tmp_path / "landing" / ingest.landing_path(m)).write_bytes(b"truncated")
    assert ingest.is_loaded(m, st) is False


# ---------- exit codes ----------

@pytest.mark.parametrize(
    "statuses,code",
    [
        (["loaded"], 0),
        (["skipped", "not_published"], 0),
        (["not_published"], 99),
        (["loaded", "failed"], 1),
        (["not_published", "failed"], 1),
    ],
)
def test_exit_code(statuses, code):
    assert ingest.exit_code(statuses) == code
