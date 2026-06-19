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
    """A scoped key/value data plane.

    A store operates within a *scope* — a sub-region of the keyspace. Scope is a
    behavioural concept here, NOT a key-shape mandate: how a backend realises
    scope (a path prefix, an S3 key prefix, a DynamoDB partition value, a table)
    is each store's private business and never part of this contract. Callers
    only ever pass logical keys and abstract scope fragments.

    Two scope-related capabilities:
      - ``subscope`` (access limiting): return a NEW store narrowed to a deeper
        scope, for handing to a less-trusted holder (engine -> node). Append-only
        — you can narrow, never widen.
      - operations may take a descend ``at`` fragment to act at a deeper sub-scope
        within the store's own authority WITHOUT constructing a new store.
    """

    @abstractmethod
    def put(self, key: str, data: bytes, at: tuple[str, ...] = ()) -> str: ...

    @abstractmethod
    def get(self, key: str, at: tuple[str, ...] = ()) -> bytes: ...

    @abstractmethod
    def has(self, key: str, at: tuple[str, ...] = ()) -> bool: ...

    @abstractmethod
    def subscope(self, fragments: tuple[str, ...]) -> "Store":
        """Return a new store narrowed to ``self.scope + fragments`` (access
        limiting). Append-only; the realisation of the narrowing is private."""
        ...

    # convenience JSON helpers
    def put_json(self, key: str, obj: Any, at: tuple[str, ...] = ()) -> str:
        return self.put(key, json.dumps(obj).encode("utf-8"), at=at)

    def get_json(self, key: str, at: tuple[str, ...] = ()) -> Any:
        return json.loads(self.get(key, at=at).decode("utf-8"))

    def copy(self, src_key: str, dst_store: "Store", dst_key: str,
             from_: tuple[str, ...] = (), into: tuple[str, ...] = ()) -> str:
        """Copy a key to another store. Both endpoints may descend (``from_`` on
        the source, ``into`` on the destination) — symmetric, since both the
        source and destination are typically at descended sub-scopes. Default is
        get+put (works across any backends); backends override for same-backend
        server-side fast paths."""
        dst_store.put(dst_key, self.get(src_key, at=from_), at=into)
        return dst_key


class FileStore(Store):
    """A local directory store. Scope is realised as nested path segments under
    ``root`` — but that's this backend's private choice, not the contract."""

    def __init__(self, root: str | Path, scope: tuple[str, ...] = ()):
        # base root and scope are kept distinct (the subprocess runner rewrites
        # root to the container mount while preserving scope); this store folds
        # them at addressing time, privately.
        self.base = Path(root)
        self.scope = tuple(scope)
        # ensure the scoped root exists
        self._build_path("").parent.mkdir(parents=True, exist_ok=True)

    # --- private: this backend realises scope as nested path segments -------- #
    def _build_path(self, key: str, at: tuple[str, ...] = ()) -> Path:
        # held scope + per-op descend + key. Private to FileStore; the contract
        # says nothing about scope being part of the path.
        return self.base.joinpath(*self.scope, *at, key)

    def subscope(self, fragments: tuple[str, ...]) -> "FileStore":
        return FileStore(self.base, scope=self.scope + tuple(fragments))

    def put(self, key: str, data: bytes, at: tuple[str, ...] = ()) -> str:
        p = self._build_path(key, at)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
        return key

    def get(self, key: str, at: tuple[str, ...] = ()) -> bytes:
        return self._build_path(key, at).read_bytes()

    def has(self, key: str, at: tuple[str, ...] = ()) -> bool:
        return self._build_path(key, at).exists()

    def list(self, prefix: str = "", at: tuple[str, ...] = ()) -> list[str]:
        """List keys under a prefix (relative to the scoped root)."""
        scoped_root = self._build_path("", at)
        base = scoped_root / prefix
        if not base.exists():
            return []
        out = []
        for p in base.rglob("*"):
            if p.is_file():
                out.append(str(p.relative_to(scoped_root)))
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
                 scope: tuple[str, ...] = ()):
        if not bucket:
            raise ValueError("S3Store requires a bucket")
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self.scope = tuple(scope)
        self.region = region
        self._client = None  # lazy: only build the boto3 client when first used

    @property
    def client(self):
        if self._client is None:
            import boto3
            self._client = boto3.client("s3", region_name=self.region)
        return self._client

    # --- private: this backend realises scope as S3 key-prefix segments ------ #
    def _build_key(self, key: str, at: tuple[str, ...] = ()) -> str:
        # prefix + held scope + per-op descend + key, '/'-joined. Private to
        # S3Store; the contract does not mandate scope-as-key.
        parts = [p for p in (self.prefix, *self.scope, *at, key) if p]
        return "/".join(parts)

    def subscope(self, fragments: tuple[str, ...]) -> "S3Store":
        s = S3Store(self.bucket, prefix=self.prefix, region=self.region,
                    scope=self.scope + tuple(fragments))
        s._client = self._client   # share the lazy client
        return s

    def put(self, key: str, data: bytes, at: tuple[str, ...] = ()) -> str:
        self.client.put_object(Bucket=self.bucket, Key=self._build_key(key, at), Body=data)
        return key

    def get(self, key: str, at: tuple[str, ...] = ()) -> bytes:
        resp = self.client.get_object(Bucket=self.bucket, Key=self._build_key(key, at))
        return resp["Body"].read()

    def has(self, key: str, at: tuple[str, ...] = ()) -> bool:
        from botocore.exceptions import ClientError
        try:
            self.client.head_object(Bucket=self.bucket, Key=self._build_key(key, at))
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def list(self, prefix: str = "", at: tuple[str, ...] = ()) -> list[str]:
        """List keys under a prefix (scoped; the scope+prefix stripped back off)."""
        full = self._build_key(prefix, at)
        scoped_base = self._build_key("", at)            # everything under the scope
        strip = (scoped_base + "/") if scoped_base else ""
        paginator = self.client.get_paginator("list_objects_v2")
        out = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=full):
            for obj in page.get("Contents", []):
                k = obj["Key"]
                out.append(k[len(strip):] if strip and k.startswith(strip) else k)
        return out

    def copy(self, src_key: str, dst_store: "Store", dst_key: str,
             from_: tuple[str, ...] = (), into: tuple[str, ...] = ()) -> str:
        # same-backend fast path: server-side CopyObject (no bytes through us)
        if isinstance(dst_store, S3Store) and dst_store.bucket == self.bucket:
            self.client.copy_object(
                Bucket=dst_store.bucket,
                Key=dst_store._build_key(dst_key, into),
                CopySource={"Bucket": self.bucket, "Key": self._build_key(src_key, from_)},
            )
            return dst_key
        return super().copy(src_key, dst_store, dst_key, from_=from_, into=into)
