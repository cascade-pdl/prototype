from __future__ import annotations
import os
import threading
from contextlib import contextmanager
from dataclasses import dataclass, asdict, replace
from typing import Any, Self

from cascade.store.base import (
    MAX_IN_FLIGHT, BlockingStore, KeyNotFound, Store, StoreConfig, StoreError, blocking,
)


_MISSING = ("404", "NoSuchKey", "NotFound")
"""Error codes for an absent key: ``GetObject`` says ``NoSuchKey``, but ``HeadObject`` has
no body to say it in, so botocore reports the bare status."""


@dataclass
class S3Config(StoreConfig):
    bucket: str
    prefix: str = ""
    region: str | None = None
    scope: tuple[str, ...] = ()
    endpoint: str | None = None
    """An S3-compatible service's URL (garage, MinIO); absent for AWS itself, whose
    endpoint boto derives from the region."""

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


class S3Store(BlockingStore):
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
        self.endpoint = config.endpoint
        self._client = None  # lazy: the process's client for this region/endpoint

    @property
    def client(self):
        if self._client is None:
            self._client = _shared_client(self.region, self.endpoint)
        return self._client

    def _build_key(self, key: str | None = None, at: tuple[str, ...] = ()) -> str:
        parts = [p for p in (self.prefix, *self.scope, *at, key) if p]
        return "/".join(parts)

    def _put(self, key: str, data: bytes, at: tuple[str, ...] = ()) -> str:
        s3_key = self._build_key(key, at)
        with self._errors(s3_key):
            self.client.put_object(Bucket=self.bucket, Key=s3_key, Body=data)
        return key

    def _get(self, key: str, at: tuple[str, ...] = ()) -> bytes:
        s3_key = self._build_key(key, at)
        with self._errors(s3_key, missing=KeyNotFound):
            return self.client.get_object(Bucket=self.bucket, Key=s3_key)["Body"].read()

    def _has(self, key: str, at: tuple[str, ...] = ()) -> bool:
        s3_key = self._build_key(key, at)
        try:
            with self._errors(s3_key, missing=KeyNotFound):
                self.client.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except KeyNotFound:
            return False

    def _list(self, at: tuple[str, ...] = ()) -> list[str]:
        """List keys under a prefix (scoped; the scope+prefix stripped back off)."""
        scoped_base = self._build_key(at=at)
        strip = (scoped_base + "/") if scoped_base else ""
        out = []
        with self._errors(scoped_base):
            paginator = self.client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=scoped_base):
                for obj in page.get("Contents", []):
                    k = obj["Key"]
                    out.append(k[len(strip) :] if strip and k.startswith(strip) else k)
        return [p for p in out if p]

    def _check(self) -> None:
        """An identity resolves (locally, no request), then one ``HeadBucket``: the
        bucket exists, in this region or at this endpoint, and the identity may use it."""
        import boto3

        if boto3.Session().get_credentials() is None:
            raise StoreError(
                f"no AWS credentials for the store s3://{self.bucket}: set AWS_PROFILE, "
                "or export AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY "
                '(for the dev garage: eval "$(cascade dev garage env)")'
            )
        with self._errors(""):
            self.client.head_bucket(Bucket=self.bucket)

    @contextmanager
    def _errors(self, s3_key: str, missing: type[Exception] | None = None):
        """botocore's failures as cascade's: an absent key as ``missing`` when the call
        has one, anything else as a one-line :class:`StoreError` saying what to fix."""
        from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

        where = f"s3://{self.bucket}/{s3_key}" if s3_key else f"s3://{self.bucket}"
        try:
            yield
        except ClientError as e:
            code = e.response["Error"]["Code"]
            status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if missing is not None and code in _MISSING:
                raise missing(where) from None
            if code in ("NoSuchBucket",) or (not s3_key and status == 404):
                raise StoreError(f"{where}: no such bucket") from None
            if code in ("AccessDenied", "403") or status == 403:
                raise StoreError(
                    f"{where}: access denied for this identity (check AWS_PROFILE and the "
                    "bucket's permissions)"
                ) from None
            if status == 301 or code in ("PermanentRedirect", "AuthorizationHeaderMalformed"):
                raise StoreError(
                    f"{where}: the bucket is in another region than {self.region!r}"
                ) from None
            raise StoreError(f"{where}: {code}: {e.response['Error'].get('Message', '')}") from None
        except NoCredentialsError:
            raise StoreError(
                f"{where}: no AWS credentials: set AWS_PROFILE, or export "
                "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
            ) from None
        except BotoCoreError as e:
            raise StoreError(f"{where}: {e}") from None

    async def copy(
        self,
        src_key: str,
        dst_key: str,
        from_: tuple[str, ...] = (),
        to_: tuple[str, ...] = (),
        dst_store: "Store | None" = None,
    ) -> str:
        # same-backend fast path: server-side CopyObject (no bytes through us)
        dst_store = dst_store or self
        if isinstance(dst_store, S3Store) and dst_store.bucket == self.bucket:
            await blocking(self._copy_within, src_key, dst_key, from_, to_, dst_store)
            return dst_key
        return await super().copy(
            src_key,
            dst_key,
            from_=from_,
            to_=to_,
            dst_store=dst_store,
        )

    def _copy_within(
        self,
        src_key: str,
        dst_key: str,
        from_: tuple[str, ...],
        to_: tuple[str, ...],
        dst_store: "S3Store",
    ) -> None:
        src = self._build_key(src_key, from_)
        with self._errors(src, missing=KeyNotFound):
            self.client.copy_object(
                Bucket=dst_store.bucket,
                Key=dst_store._build_key(dst_key, to_),
                CopySource={"Bucket": self.bucket, "Key": src},
            )


_clients: dict[tuple[str | None, ...], Any] = {}
_clients_lock = threading.Lock()

_IDENTITY_ENV = ("AWS_PROFILE", "AWS_ACCESS_KEY_ID", "AWS_CONTAINER_CREDENTIALS_FULL_URI")
"""What picks the identity a new client resolves. Part of the cache key, so a process
whose environment names another identity (a test switching stores) gets a new client
rather than the old identity."""


def _shared_client(region: str | None, endpoint: str | None):
    """One boto3 client per region, endpoint and identity for the whole process.

    A fan makes a store per lane, so a client per store would be a thousand clients for
    a thousand lanes, each with its own connections. boto3 clients are thread-safe;
    sessions are not, which is why creating one is behind a lock. The connection pool
    matches the store's thread pool, so every call in flight has a connection.
    """
    key = (region, endpoint, *(os.environ.get(name) for name in _IDENTITY_ENV))
    with _clients_lock:
        client = _clients.get(key)
        if client is None:
            import boto3
            from botocore.config import Config

            # a session of its own: boto3's default session keeps the first credentials
            # it resolves, which would hand a new identity's client the old keys
            client = boto3.Session().client(
                "s3", region_name=region, endpoint_url=endpoint,
                config=Config(max_pool_connections=MAX_IN_FLIGHT),
            )
            _clients[key] = client
        return client
