from __future__ import annotations

from enum import Enum
from typing import Type, Any

from cascade.store.base import Store, StoreConfig
from cascade.store.s3_store import S3Store, S3Config
from cascade.store.file_store import FileStore, FileConfig


class StoreKind(Enum):
    S3 = "s3"
    FILE = "file"


RegEntry = tuple[StoreKind, Type[Store], Type[StoreConfig]]


STORE_REG: list[RegEntry] = [
    (StoreKind.FILE, FileStore, FileConfig),
    (StoreKind.S3, S3Store, S3Config),
]
_BY_KIND = {entry[0]: entry for entry in STORE_REG}
assert len(_BY_KIND) == len(STORE_REG), "duplicate StoreKind in STORE_REG"


def from_store(instance: Store) -> RegEntry:
    for entry in STORE_REG:
        _kind, store_cls, _config_cls = entry
        if isinstance(instance, store_cls):
            return entry
    raise RuntimeError(
        f"instance of type {type(instance)} not a valid registered store"
    )


def from_config(instance: StoreConfig) -> RegEntry:
    for entry in STORE_REG:
        _kind, _store_cls, config_cls = entry
        if isinstance(instance, config_cls):
            return entry
    raise RuntimeError(
        f"instance of type {type(instance)} not a valid registered store config"
    )


def from_kind(kind: StoreKind) -> RegEntry:
    return _BY_KIND[kind]


def encode_config(config: StoreConfig) -> dict[str, Any]:
    """An inert ``StoreConfig`` as a ``{kind, config}`` block, tagged by backend.
 
    The config-level twin of :func:`encode`. The deployment loader wants this
    form — a spec, not a live store — while sharing the one envelope defined here.
    """
    kind, *_ = from_config(config)
    return {"kind": kind.value, "config": config.encode()}
 
 
def decode_config(raw: dict[str, Any]) -> StoreConfig:
    """Decode a ``{kind, config}`` block to the concrete ``StoreConfig`` — no live
    ``Store`` is built (no mkdir / no boto client). The twin of :func:`decode`."""
    kind = StoreKind(raw["kind"])
    _kind, _store_cls, config_cls = from_kind(kind)
    return config_cls.decode(raw["config"])
 
 
def encode(store: Store) -> dict[str, Any]:
    """A live ``Store`` as a ``{kind, config}`` block (used for CASCADE_STORE_CONF)."""
    return encode_config(store.config)
 
 
def decode(raw: dict[str, Any]) -> Store:
    """Decode a ``{kind, config}`` block to a live ``Store`` (materialises the backend)."""
    config = decode_config(raw)
    _kind, store_cls, _config_cls = from_config(config)
    return store_cls(config)
