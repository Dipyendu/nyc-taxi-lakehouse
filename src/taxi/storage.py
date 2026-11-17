"""Landing storage: a Unity Catalog volume (Files API) or a local directory."""

from __future__ import annotations

import io
from pathlib import Path
from typing import Protocol


class Storage(Protocol):
    def size(self, rel: str) -> int | None: ...
    def read_bytes(self, rel: str) -> bytes | None: ...
    def write_bytes(self, rel: str, data: bytes) -> None: ...
    def upload(self, local: Path, rel: str) -> None: ...


class LocalStorage:
    def __init__(self, root: str | Path):
        self.root = Path(root)

    def _p(self, rel: str) -> Path:
        return self.root / rel

    def size(self, rel: str) -> int | None:
        p = self._p(rel)
        return p.stat().st_size if p.is_file() else None

    def read_bytes(self, rel: str) -> bytes | None:
        p = self._p(rel)
        return p.read_bytes() if p.is_file() else None

    def write_bytes(self, rel: str, data: bytes) -> None:
        p = self._p(rel)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

    def upload(self, local: Path, rel: str) -> None:
        p = self._p(rel)
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(local, "rb") as src, open(p, "wb") as dst:
            while chunk := src.read(1 << 20):
                dst.write(chunk)


class VolumeStorage:
    """Files API on /Volumes/<catalog>/<schema>/<volume>/...; credentials from the environment."""

    def __init__(self, root: str, client=None):
        from databricks.sdk.errors import NotFound

        from taxi.config import workspace_client

        self.root = root.rstrip("/")
        self.w = client or workspace_client()
        self._not_found = NotFound

    def _p(self, rel: str) -> str:
        return f"{self.root}/{rel}"

    def size(self, rel: str) -> int | None:
        try:
            return self.w.files.get_metadata(self._p(rel)).content_length
        except self._not_found:
            return None

    def read_bytes(self, rel: str) -> bytes | None:
        try:
            return self.w.files.download(self._p(rel)).contents.read()
        except self._not_found:
            return None

    def write_bytes(self, rel: str, data: bytes) -> None:
        self.w.files.upload(self._p(rel), io.BytesIO(data), overwrite=True)

    def upload(self, local: Path, rel: str) -> None:
        with open(local, "rb") as f:
            self.w.files.upload(self._p(rel), f, overwrite=True)


def storage_for(root: str) -> Storage:
    return VolumeStorage(root) if root.startswith("/Volumes/") else LocalStorage(root)
