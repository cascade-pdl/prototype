from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import Any, Self


class StoreConfig(ABC):

    scope: tuple[str, ...]

    def subscope(self, scope: tuple[str, ...]) -> Self:
        raw = self.encode()
        raw["scope"] = (*self.scope, *scope)
        return self.__class__.decode(raw)

    @abstractmethod
    def encode(self) -> dict[str, Any]: ...

    @classmethod
    @abstractmethod
    def decode(cls, raw: dict[str, Any]) -> Self: ...


class Store(ABC):
    """A scoped key/value data plane.

    A store operates within a *scope* — a sub-region of the keyspace. Scope is a
    behavioural concept here, NOT a key-shape mandate: how a backend realises
    scope (a path prefix, an S3 key prefix, a DynamoDB partition value, a table)
    is each store's private business and never part of this contract. Callers
    only ever pass logical keys and abstract scope fragments.
    """

    config: StoreConfig

    @abstractmethod
    def put(self, key: str, data: bytes, at: tuple[str, ...] = ()) -> str: ...

    @abstractmethod
    def get(self, key: str, at: tuple[str, ...] = ()) -> bytes: ...

    @abstractmethod
    def has(self, key: str, at: tuple[str, ...] = ()) -> bool: ...

    @abstractmethod
    def list(self, at: tuple[str, ...] = ()) -> list[str]: ...

    # convenience JSON helpers
    def put_json(self, key: str, obj: Any, at: tuple[str, ...] = ()) -> str:
        return self.put(key, json.dumps(obj).encode("utf-8"), at=at)

    def get_json(self, key: str, at: tuple[str, ...] = ()) -> Any:
        return json.loads(self.get(key, at=at).decode("utf-8"))

    def copy(
        self,
        src_key: str,
        dst_key: str,
        from_: tuple[str, ...] = (),
        to_: tuple[str, ...] = (),
        dst_store: "Store | None" = None,
    ) -> str:
        """Copy a key to another store."""
        dst_store = dst_store or self
        return dst_store.put(
            key=dst_key,
            data=self.get(key=src_key, at=from_),
            at=to_,
        )
