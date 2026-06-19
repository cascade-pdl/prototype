"""The data plane: a content/key store.

The store is an *interface* — ``put``/``get``/``has``/``put_json``/``get_json``
over string keys — with swappable backends. The runner and engine move only
*keys* through this store; payloads never pass through the coordinator.

Two backends:
  - :class:`FileStore`  — a local directory. The whole local mode.
  - :class:`S3Store`     — points at an S3 bucket/prefix (sync boto3).

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

    def __init__(self, root: str | Path, scope: str | None = None):
        # scope narrows the keyspace to a sub-region; for a file store it's a
        # path segment under root. The config keeps root and scope separate (so
        # the subprocess runner can rewrite root while preserving scope); once
        # built, the store folds them into one effective root.
        self.root = Path(root) / scope if scope else Path(root)
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

    def list(self, prefix: str = "") -> list[str]:
        """List keys under a prefix (store-relative)."""
        base = self.root / prefix
        if not base.exists():
            return []
        out = []
        for p in base.rglob("*"):
            if p.is_file():
                out.append(str(p.relative_to(self.root)))
        return out


class S3Store(Store):
    """An S3-backed store. Sync boto3 — fine because the engine only does small
    control-plane reads/writes here (run-state, manifests); the *bulk* payloads
    move container<->S3 directly via the node-side fetch/stage utilities, never
    through the engine.

    The interface is identical to :class:`FileStore`, which is the point: the
    engine and runner don't care which backend they talk to.
    """

    def __init__(self, bucket: str, prefix: str = "", region: str | None = None,
                 scope: str | None = None):
        if not bucket:
            raise ValueError("S3Store requires a bucket")
        self.bucket = bucket
        # scope narrows the keyspace; for S3 it folds into the effective prefix.
        # The config keeps prefix and scope separate (so the rigging can rewrite
        # one without disturbing the other); the built store folds them.
        eff = f"{prefix.rstrip('/')}/{scope}" if (prefix and scope) else (scope or prefix)
        self.prefix = (eff or "").rstrip("/")
        self.region = region
        self._client = None  # lazy: only build the boto3 client when first used

    @property
    def client(self):
        if self._client is None:
            import boto3
            self._client = boto3.client("s3", region_name=self.region)
        return self._client

    def _key(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    def put(self, key: str, data: bytes) -> str:
        self.client.put_object(Bucket=self.bucket, Key=self._key(key), Body=data)
        return key

    def get(self, key: str) -> bytes:
        resp = self.client.get_object(Bucket=self.bucket, Key=self._key(key))
        return resp["Body"].read()

    def has(self, key: str) -> bool:
        from botocore.exceptions import ClientError
        try:
            self.client.head_object(Bucket=self.bucket, Key=self._key(key))
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def list(self, prefix: str = "") -> list[str]:
        """List keys under a prefix (store-relative, prefix stripped back off)."""
        full = self._key(prefix)
        paginator = self.client.get_paginator("list_objects_v2")
        out = []
        strip = (self.prefix + "/") if self.prefix else ""
        for page in paginator.paginate(Bucket=self.bucket, Prefix=full):
            for obj in page.get("Contents", []):
                k = obj["Key"]
                out.append(k[len(strip):] if strip and k.startswith(strip) else k)
        return out
