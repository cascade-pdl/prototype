"""The data plane: a content/key store.

The store is an *interface* — ``put``/``get``/``has``/``put_json``/``get_json``
over string keys — with swappable backends. The runner and engine move only
*keys* through this store; payloads never pass through the coordinator.

Two backends:
  - :class:`FileStore`  — a local directory. The whole local mode.
  - :class:`S3Store`     — points at an S3 bucket/prefix (stub; fill in boto3).

Keys are run-scoped paths like ``runs/<run_id>/<node_id>/<instance>/output.json``.
(Content-addressing — keying by content hash for dedup — is an optimisation
deferred to later; run-scoped keys are enough to prove the concept.)
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any


class Store(ABC):
    @abstractmethod
    def put(self, key: str, data: bytes) -> str: ...

    @abstractmethod
    def get(self, key: str) -> bytes: ...

    @abstractmethod
    def has(self, key: str) -> bool: ...

    # convenience JSON helpers (payloads here are JSON for simplicity)
    def put_json(self, key: str, obj: Any) -> str:
        return self.put(key, json.dumps(obj).encode("utf-8"))

    def get_json(self, key: str) -> Any:
        return json.loads(self.get(key).decode("utf-8"))


class FileStore(Store):
    """A local directory store. ``root/<key>`` is the file for ``key``."""

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        p = self.root / key
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    def put(self, key: str, data: bytes) -> str:
        self._path(key).write_bytes(data)
        return key

    def get(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def has(self, key: str) -> bool:
        return (self.root / key).exists()


class S3Store(Store):
    """An S3-backed store. Stubbed — fill in with boto3 for distributed runs.

    The interface is identical to :class:`FileStore`, which is the point: the
    engine and runner don't care which backend they talk to.
    """

    def __init__(self, bucket: str, prefix: str = ""):
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")

    def _key(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    def put(self, key: str, data: bytes) -> str:  # pragma: no cover - stub
        # import boto3; boto3.client("s3").put_object(Bucket=..., Key=..., Body=data)
        raise NotImplementedError("S3Store.put: wire up boto3 for distributed runs")

    def get(self, key: str) -> bytes:  # pragma: no cover - stub
        raise NotImplementedError("S3Store.get: wire up boto3 for distributed runs")

    def has(self, key: str) -> bool:  # pragma: no cover - stub
        raise NotImplementedError("S3Store.has: wire up boto3 for distributed runs")
