from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import Any, Self, Sequence

from cascade.store.collection import (
    MAX_DEPTH,
    CollectionDescriptor,
    CollectionError,
)


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

    # --- collections ------------------------------------------------------
    # One logical key, many stored objects. Concrete on the ABC and built only over
    # put/get, so every backend -- including the in-memory test double -- gets them.

    def _read(
        self,
        key: str,
        *,
        at: tuple[str, ...],
        as_json: bool,
        depth: int,
    ):

        if depth > MAX_DEPTH:
            raise CollectionError(
                f"collection nesting exceeded {MAX_DEPTH} at {'/'.join((*at, key))}; "
                "a descriptor probably references itself"
            )
        raw = self.get(key, at=at)
        if descriptor := CollectionDescriptor.try_decode(raw):
            return [
                self._read(element_key, at=(*at, *scope), as_json=as_json, depth=depth + 1)
                for scope, element_key in descriptor.elements
            ]
        return json.loads(raw.decode("utf-8")) if as_json else raw

    def read(self, key: str, at: tuple[str, ...] = ()) -> bytes | list:
        return self._read(key, at=at, as_json=False, depth=0)

    def read_json(self, key: str, at: tuple[str, ...] = ()) -> Any:
        return self._read(key, at=at, as_json=True, depth=0)

    def write_collection(
        self,
        key: str,
        elements: "Sequence[tuple[tuple[str, ...], str]]",
        at: tuple[str, ...] = (),
        element_type: str | None = None,
    ) -> str:
        """Write a descriptor referencing elements already in this store.

        Element scopes are relative to **the descriptor's own location** (``at``), so the
        result can be read back through a store scoped at any depth above it.

        The distributed form: no payload is copied, so gathering a fan costs one small
        object regardless of how large the elements are.
        """

        descriptor = CollectionDescriptor(
            elements=tuple((tuple(scope), k) for scope, k in elements),
            element_type=element_type,
        )
        return self.put_json(key, descriptor.encode(), at=at)

    def width(self, key: str, at: tuple[str, ...] = ()) -> int:
        raw = self.get(key, at=at)
        if descriptor := CollectionDescriptor.try_decode(raw):
            return descriptor.count
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise CollectionError(
                f"{'/'.join((*at, key))} is not a collection: opaque bytes with no "
                "descriptor, so its width needs the port's declared encoding"
            )
        if isinstance(value, list):
            return len(value)
        raise CollectionError(f"{'/'.join((*at, key))} is not a collection")

    def is_collection(self, key: str, at: tuple[str, ...] = ()) -> bool:
        return CollectionDescriptor.try_decode(self.get(key, at=at)) is not None
