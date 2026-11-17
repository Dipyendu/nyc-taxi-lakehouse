"""Ingest one NYC yellow-taxi month: download (streamed), validate, upload, write a manifest.

Usage: python -m taxi.ingest 2025-01 [--to 2025-03] [--force]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow.parquet as pq
import requests

from taxi import config
from taxi.storage import Storage, storage_for

CHUNK = 1 << 20


class NotPublished(Exception):
    """The source returned 403/404: the month is not (yet) published."""


class SizeMismatch(Exception):
    """Bytes received differ from Content-Length, or the server sent no Content-Length."""


class ServerError(Exception):
    """HTTP 5xx from the source; worth retrying."""


class SourceMissing(Exception):
    """403/404 for a month old enough that it must exist: a failure, not "not published yet"."""


# A month at most this many months before the current month may still be unpublished
# (files appeared up to 83 days after their month: docs/evidence/a8/b1-publication-lag.txt).
UNPUBLISHED_WINDOW_MONTHS = 6


@dataclass(frozen=True, order=True)
class Month:
    year: int
    month: int

    @classmethod
    def parse(cls, text: str) -> Month:
        y, m = text.split("-")
        if not (1 <= int(m) <= 12) or len(y) != 4:
            raise ValueError(f"bad month {text!r}, expected YYYY-MM")
        return cls(int(y), int(m))

    def next(self) -> Month:
        return Month(self.year + self.month // 12, self.month % 12 + 1)

    def __str__(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"


def month_range(start: Month, end: Month) -> list[Month]:
    if end < start:
        raise ValueError(f"end {end} before start {start}")
    out, m = [], start
    while m <= end:
        out.append(m)
        m = m.next()
    return out


def file_name(m: Month) -> str:
    return f"yellow_tripdata_{m}.parquet"


def source_url(m: Month) -> str:
    return f"{config.SOURCE_BASE_URL}/{file_name(m)}"


def landing_path(m: Month) -> str:
    return f"yellow/year={m.year:04d}/month={m.month:02d}/{file_name(m)}"


def manifest_path(m: Month) -> str:
    return f"_manifest/yellow/{m}.json"


def log(msg: str) -> None:
    """Progress line on stderr, flushed at once; stdout carries one JSON result per month."""
    print(f"[{datetime.now():%H:%M:%S}] {msg}", file=sys.stderr, flush=True)


def with_retries(fn, what: str, retry_on: tuple, attempts: int = config.ATTEMPTS, sleep=time.sleep):
    """Call fn up to `attempts` times, pausing 2 s, 4 s, ... between tries; re-raise the last error."""
    for i in range(1, attempts + 1):
        try:
            return fn()
        except retry_on as e:
            if i == attempts:
                log(f"{what}: attempt {i}/{attempts} failed ({type(e).__name__}); giving up")
                raise
            log(f"{what}: attempt {i}/{attempts} failed ({type(e).__name__}: {e}); retrying in {2**i} s")
            sleep(2**i)


DOWNLOAD_RETRY_ON = (
    requests.ConnectionError,
    requests.Timeout,
    requests.exceptions.ChunkedEncodingError,
    ServerError,
    SizeMismatch,
)
UPLOAD_RETRY_ON = (Exception,)  # SDK and socket errors alike; bounded by `attempts`


def download(url: str, dest: Path, session) -> int:
    """Stream url to dest via a .part file; rename only after the size check passes."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    timeout = (config.HTTP_CONNECT_TIMEOUT_S, config.HTTP_READ_TIMEOUT_S)
    with session.get(url, stream=True, timeout=timeout) as r:
        if r.status_code in (403, 404):
            raise NotPublished(f"{url} -> HTTP {r.status_code}")
        if r.status_code >= 500:
            raise ServerError(f"{url} -> HTTP {r.status_code}")
        r.raise_for_status()
        if "Content-Length" not in r.headers:
            raise SizeMismatch(f"{url}: no Content-Length header")
        expected = int(r.headers["Content-Length"])
        got = 0
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(CHUNK):
                f.write(chunk)
                got += len(chunk)
    if got != expected:
        tmp.unlink()
        raise SizeMismatch(f"{url}: got {got} bytes, Content-Length {expected}")
    tmp.rename(dest)
    return got


def parquet_facts(path: Path) -> dict:
    """Row count and schema from Parquet metadata only; no data pages are read."""
    pf = pq.ParquetFile(path)
    schema = [[f.name, str(f.type)] for f in pf.schema_arrow]
    return {
        "rows": pf.metadata.num_rows,
        "schema": schema,
        "schema_hash": hashlib.sha256(json.dumps(schema).encode()).hexdigest()[:16],
    }


def is_loaded(m: Month, storage: Storage) -> bool:
    raw = storage.read_bytes(manifest_path(m))
    if raw is None:
        return False
    return storage.size(landing_path(m)) == json.loads(raw)["bytes"]


def months_ago(m: Month, today: date) -> int:
    return (today.year * 12 + today.month) - (m.year * 12 + m.month)


def ingest_month(
    m: Month, storage: Storage, session, work_dir: Path, force: bool = False, sleep=time.sleep, today=None
) -> dict:
    today = today or date.today()
    if not force and is_loaded(m, storage):
        return {"month": str(m), "status": "skipped"}
    local = work_dir / "raw" / file_name(m)
    if local.is_file() and not force:
        nbytes = local.stat().st_size  # only a size-checked download is ever renamed into place
        log(f"{m}: using local copy, {nbytes} bytes")
    else:
        log(f"{m}: downloading {source_url(m)}")
        t = time.monotonic()
        try:
            nbytes = with_retries(
                lambda: download(source_url(m), local, session), f"{m} download", DOWNLOAD_RETRY_ON, sleep=sleep
            )
        except NotPublished as e:
            if months_ago(m, today) > UNPUBLISHED_WINDOW_MONTHS:
                raise SourceMissing(f"{e}; {m} is {months_ago(m, today)} months old and should exist") from e
            log(f"{m}: not published")
            return {"month": str(m), "status": "not_published", "detail": str(e)}
        log(f"{m}: downloaded {nbytes} bytes in {time.monotonic() - t:.1f} s")
    facts = parquet_facts(local)
    log(f"{m}: uploading to {landing_path(m)}")
    t = time.monotonic()
    with_retries(lambda: storage.upload(local, landing_path(m)), f"{m} upload", UPLOAD_RETRY_ON, sleep=sleep)
    log(f"{m}: uploaded in {time.monotonic() - t:.1f} s")
    record = {
        "month": str(m),
        "file": file_name(m),
        "landing_path": landing_path(m),
        "bytes": nbytes,
        "rows": facts["rows"],
        "schema_hash": facts["schema_hash"],
        "schema": facts["schema"],
        "uploaded_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    body = json.dumps(record, indent=1).encode()
    with_retries(lambda: storage.write_bytes(manifest_path(m), body), f"{m} manifest", UPLOAD_RETRY_ON, sleep=sleep)
    log(f"{m}: manifest written ({facts['rows']} rows)")
    return {"month": str(m), "status": "loaded", "bytes": nbytes, "rows": facts["rows"]}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("month", type=Month.parse)
    p.add_argument("--to", type=Month.parse, default=None)
    p.add_argument("--force", action="store_true")
    a = p.parse_args(argv)
    storage = storage_for(config.LANDING_ROOT)
    statuses = []
    with requests.Session() as s:
        for m in month_range(a.month, a.to or a.month):
            try:
                out = ingest_month(m, storage, s, config.LOCAL_DATA_DIR, a.force)
            except (SizeMismatch, ServerError, SourceMissing, requests.RequestException) as e:
                out = {"month": str(m), "status": "failed", "detail": str(e)}
            print(json.dumps(out), flush=True)
            statuses.append(out["status"])
    return exit_code(statuses)


def exit_code(statuses: list[str]) -> int:
    """1 if any month failed; 99 (Airflow BashOperator's skip code) if none was published; else 0."""
    if "failed" in statuses:
        return 1
    if statuses and all(s == "not_published" for s in statuses):
        return 99
    return 0


if __name__ == "__main__":
    sys.exit(main())
