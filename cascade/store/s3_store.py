from __future__ import annotations
from dataclasses import dataclass, asdict, replace
from typing import Any, Self

from cascade.store.base import Store, StoreConfig


@dataclass
class S3Config(StoreConfig):
    bucket: str
    prefix: str = ""
    region: str | None = None
    scope: tuple[str, ...] = ()

    def subscope(self, scope: tuple[str, ...]):
        return replace(self, scope=(*self.scope, *scope))

    def encode(self) -> dict[str, Any]:
        data = asdict(self)  # all fields are json compatible except scope
        data["scope"] = list(data["scope"])
        return data

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        raw = dict(raw)
        scope = tuple(raw.pop("scope", ()))
        return cls(scope=scope, **raw)


class S3Store(Store):
    """An S3-backed store. Sync boto3 — fine because the engine only does small
    control-plane reads/writes here (run-state, manifests); the *bulk* payloads
    move container<->S3 directly via the node-side fetch/stage utilities, never
    through the engine.

    The interface is identical to :class:`FileStore`, which is the point: the
    engine and runner don't care which backend they talk to.
    """

    def __init__(
        self,
        config: S3Config,
    ):
        self.config = config
        self.bucket = config.bucket
        self.prefix = config.prefix.rstrip("/")
        self.scope = tuple(config.scope)
        self.region = config.region
        self._client = None  # lazy: only build the boto3 client when first used

    @property
    def client(self):
        if self._client is None:
            import boto3

            self._client = boto3.client("s3", region_name=self.region)
        return self._client

    def _build_key(self, key: str | None = None, at: tuple[str, ...] = ()) -> str:
        parts = [p for p in (self.prefix, *self.scope, *at, key) if p]
        return "/".join(parts)

    def put(self, key: str, data: bytes, at: tuple[str, ...] = ()) -> str:
        self.client.put_object(
            Bucket=self.bucket, Key=self._build_key(key, at), Body=data
        )
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

    def list(self, at: tuple[str, ...] = ()) -> list[str]:
        """List keys under a prefix (scoped; the scope+prefix stripped back off)."""
        scoped_base = self._build_key(at=at)
        strip = (scoped_base + "/") if scoped_base else ""
        paginator = self.client.get_paginator("list_objects_v2")
        out = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=scoped_base):
            for obj in page.get("Contents", []):
                k = obj["Key"]
                out.append(k[len(strip) :] if strip and k.startswith(strip) else k)
        return [p for p in out if p]

    def copy(
        self,
        src_key: str,
        dst_key: str,
        from_: tuple[str, ...] = (),
        to_: tuple[str, ...] = (),
        dst_store: "Store | None" = None,
    ) -> str:
        # same-backend fast path: server-side CopyObject (no bytes through us)
        dst_store = dst_store or self
        if isinstance(dst_store, S3Store):
            if dst_store.bucket == self.bucket:
                self.client.copy_object(
                    Bucket=dst_store.bucket,
                    Key=dst_store._build_key(dst_key, to_),
                    CopySource={
                        "Bucket": self.bucket,
                        "Key": self._build_key(src_key, from_),
                    },
                )
                return dst_key
        return super().copy(
            src_key,
            dst_key,
            from_=from_,
            to_=to_,
            dst_store=dst_store,
        )
