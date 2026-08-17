"""Collections: the two shapes a ``T[]`` artifact can take, and reading either.

A collection reaches a consumer in one of two forms:

- **monolithic** — one artifact holding all the elements. What a ref emits when it
  writes a ``T[]`` output in one go, and what ``flat-bug`` will produce.
- **distributed** — N element artifacts plus a small descriptor saying where they are.
  What a fan *could* produce, when the elements are large enough that concatenating
  them would drag every byte through the coordinator.

``read_collection`` accepts both, which is what keeps the codec off the critical path:
for JSON a monolithic collection is *already* a list once parsed, so splitting is free
and only a non-JSON encoding (item 1.7) needs real work.

The descriptor is self-identifying by a marker key, so a consumer can tell "this is a
list of detections" from "this is a pointer to twelve lists of detections" without
being told which to expect.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from cascade.store.base import Store


MARKER = "$cascade.collection"


class CollectionError(Exception):
    """An artifact could not be read as a collection."""


@dataclass(frozen=True)
class CollectionDescriptor:
    """Where the elements of a distributed collection live.

    ``elements`` are scopes relative to the same store the descriptor was read from,
    each holding ``key``. Explicit rather than derived from a pattern, so a future
    non-uniform layout (resumed lanes, mixed backends) needs no format change.
    """

    count: int
    elements: tuple[tuple[str, ...], ...]
    key: str
    element_type: str | None = None

    def encode(self) -> dict[str, Any]:
        return {
            MARKER: {
                "count": self.count,
                "elements": [list(e) for e in self.elements],
                "key": self.key,
                "element_type": self.element_type,
            }
        }

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> "CollectionDescriptor":
        body = raw[MARKER]
        return cls(
            count=body["count"],
            elements=tuple(tuple(e) for e in body["elements"]),
            key=body["key"],
            element_type=body.get("element_type"),
        )

    @staticmethod
    def looks_like(raw: Any) -> bool:
        return isinstance(raw, dict) and MARKER in raw


def read_collection(store: Store, key: str, at: tuple[str, ...] = ()) -> list[Any]:
    """Read a collection artifact as a list of element values, whichever form it took."""
    raw = store.get_json(key, at=at)
    if CollectionDescriptor.looks_like(raw):
        descriptor = CollectionDescriptor.decode(raw)
        return [store.get_json(descriptor.key, at=scope) for scope in descriptor.elements]
    if isinstance(raw, list):
        return raw
    raise CollectionError(
        f"{'/'.join((*at, key))} is neither a collection descriptor nor a list "
        f"(got {type(raw).__name__})"
    )


def collection_width(store: Store, key: str, at: tuple[str, ...] = ()) -> int:
    """The element count, reading the elements themselves only when it must."""
    raw = store.get_json(key, at=at)
    if CollectionDescriptor.looks_like(raw):
        return CollectionDescriptor.decode(raw).count
    if isinstance(raw, list):
        return len(raw)
    raise CollectionError(f"{'/'.join((*at, key))} is not a collection")