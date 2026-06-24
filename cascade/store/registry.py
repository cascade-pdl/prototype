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


def encode(store: Store) -> dict[str, Any]:
    kind, *_ = from_store(store)
    return {
        "kind": kind.value,
        "config": store.config.encode(),
    }


def decode(raw: dict[str, Any]) -> Store:
    kind = StoreKind(raw["kind"])
    _kind, store_cls, config_cls = from_kind(kind)
    assert kind == _kind
    return store_cls(config_cls.decode(raw["config"]))
