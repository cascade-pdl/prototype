"""Store vocabulary, per-kind config, and (de)serialization.

Mirrors ``runners_config``: a fixed vocabulary of store kinds, a discriminated
per-kind config, and a wrapper that round-trips to/from a JSON blob. That blob
travels from the deployment file -> engine -> the ``CASCADE_STORE_CONF`` env var
-> the container's ``cascade fetch``/``stage`` utilities, so the container builds
*the same* store the engine uses. The round-trip must be exact (a test proves
``from_json(to_json(x)) == x``), because it is the engine<->container contract.

Store config is DEPLOYMENT config (which bucket, which region) — it lives in the
deployment file alongside runners, never in the pipeline, so the pipeline stays
portable across environments.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Any


class StoreKind(str, Enum):
    file = "file"
    s3 = "s3"


@dataclass
class FileStoreConfig:
    kind: StoreKind = StoreKind.file
    root: str = "./_cascade_store"


@dataclass
class S3StoreConfig:
    kind: StoreKind = StoreKind.s3
    bucket: str = ""
    prefix: str = ""
    region: str | None = None


StoreKindConfig = FileStoreConfig | S3StoreConfig

_CONFIG_BY_KIND = {
    StoreKind.file: FileStoreConfig,
    StoreKind.s3: S3StoreConfig,
}


@dataclass
class StoreConf:
    """A store kind + its config, discriminated by ``kind``. Round-trips to a
    JSON blob for the ``CASCADE_STORE_CONF`` env var."""
    kind: StoreKind
    config: StoreKindConfig = None  # type: ignore[assignment]

    def __post_init__(self):
        if self.config is None:
            self.config = _CONFIG_BY_KIND[self.kind]()

    # --- serialization (the engine<->container contract) ------------------ #
    def to_dict(self) -> dict[str, Any]:
        c = asdict(self.config)
        # asdict turns the nested StoreKind enum into its value via the str mixin,
        # but be explicit so the blob is plain JSON-safe strings
        c["kind"] = self.kind.value
        return {"kind": self.kind.value, "config": c}

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "StoreConf":
        kind = StoreKind(raw["kind"])
        cfg_raw = dict(raw.get("config") or {})
        cfg_cls = _CONFIG_BY_KIND[kind]
        cfg_raw.pop("kind", None)
        allowed = {f for f in cfg_cls.__dataclass_fields__ if f != "kind"}
        unknown = set(cfg_raw) - allowed
        if unknown:
            raise ValueError(
                f"store config for kind '{kind.value}' has unknown field(s): "
                f"{sorted(unknown)}; allowed: {sorted(allowed)}"
            )
        return cls(kind=kind, config=cfg_cls(**cfg_raw))

    @classmethod
    def from_json(cls, blob: str) -> "StoreConf":
        return cls.from_dict(json.loads(blob))


def parse_store_conf(raw: dict[str, Any] | None) -> StoreConf:
    """Parse a deployment-file ``store:`` section. Accepts:
      store: {kind: s3, config: {bucket: ..., region: ...}}
      store: {kind: s3, bucket: ..., region: ...}   (config fields inline)
      store: file                                    (bare kind string)
      (absent) -> defaults to a FileStore
    """
    if raw is None:
        return StoreConf(kind=StoreKind.file)
    if isinstance(raw, str):
        return StoreConf(kind=StoreKind(raw))
    kind = StoreKind(raw["kind"])
    cfg_cls = _CONFIG_BY_KIND[kind]
    # config may be nested under "config" or inline as siblings of "kind"
    cfg_raw = dict(raw.get("config") or {k: v for k, v in raw.items() if k != "kind"})
    cfg_raw.pop("kind", None)
    allowed = {f for f in cfg_cls.__dataclass_fields__ if f != "kind"}
    unknown = set(cfg_raw) - allowed
    if unknown:
        raise ValueError(
            f"store config for kind '{kind.value}' has unknown field(s): "
            f"{sorted(unknown)}; allowed: {sorted(allowed)}"
        )
    return StoreConf(kind=kind, config=cfg_cls(**cfg_raw))


def build_store(conf: StoreConf):
    """Instantiate the Store for a StoreConf. Imported lazily to avoid a cycle
    (store.py imports nothing from here; here we import from store.py)."""
    from .store import FileStore, S3Store
    if conf.kind == StoreKind.file:
        return FileStore(conf.config.root)
    if conf.kind == StoreKind.s3:
        c = conf.config
        return S3Store(bucket=c.bucket, prefix=c.prefix, region=c.region)
    raise ValueError(f"no store implementation for kind '{conf.kind}'")
