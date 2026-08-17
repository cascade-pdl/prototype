"""Collections: one logical key, many stored objects.

A collection reaches a reader in one of two forms:

- **monolithic** — one artifact holding all the elements. What a ref emits when it
  writes a ``T[]`` output in one go.
- **distributed** — N element objects plus a small descriptor saying where they are.
  What a fan writes when concatenating would drag every byte through the coordinator.

``Store.read`` accepts either, so a consumer makes one call and never learns which form
it got. That is the point: the *shape* of a port is declared in the pipeline and checked
by the compiler, while the *form* is a storage decision that can change later without
touching a pipeline.

This lives in the store rather than the engine because it is addressing, not typing: a
descriptor is a set of ``(scope, key)`` pairs, which is store vocabulary throughout. The
store deliberately does **not** look inside the elements — validation already happened at
compile time, and a store that re-checked would be duplicating the compiler with less
information. A consequence worth knowing: nothing here prevents a heterogeneous
collection, and the pipeline language has no way to ask for one, so that is capability in
reserve rather than a hole.

Discrimination is in-band, by a marker key, rather than by a sidecar object. A sidecar
would need a ``has()`` probe on *every* read including scalars — on S3 that is a HEAD per
read, thousands of round trips to answer a question that is nearly always "no" — and it
would make a collection two objects, so non-atomic. The marker costs one parse of
something the caller was about to read anyway.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable, Sequence


_MARKER = "$cascade.collection"
"""Private: the descriptor format is the store's business. Callers use
``write_collection``. ``$`` is the DSL's sigil for engine-owned names (``$input``,
``$in``), and ``validate`` rejects declared names starting with it, so a ref's own output
cannot collide."""

MAX_DEPTH = 32
"""Descriptors are written by the engine and cannot cycle, but they are trivially
hand-writable; unbounded recursion against a store is an unpleasant way to find out."""


class CollectionError(Exception):
    """An artifact could not be read as a collection."""


@dataclass(frozen=True)
class CollectionDescriptor:
    """Where the elements of a distributed collection live.

    ``elements`` are ``(scope, key)`` pairs, relative to the same store the descriptor
    was read from. Pairs rather than a shared key plus a list of scopes: a fan writes the
    same port name in every lane, but *flattening* merges several descriptors whose
    elements need not agree, and paying for that generality now avoids a format migration
    later.
    """

    elements: tuple[tuple[tuple[str, ...], str], ...]
    element_type: str | None = None

    @property
    def count(self) -> int:
        return len(self.elements)

    def encode(self) -> dict[str, Any]:
        return {
            _MARKER: {
                "elements": [[list(scope), key] for scope, key in self.elements],
                "element_type": self.element_type,
            }
        }

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> "CollectionDescriptor":
        body = raw[_MARKER]
        return cls(
            elements=tuple(
                (tuple(scope), key) for scope, key in body.get("elements", ())
            ),
            element_type=body.get("element_type"),
        )

    @classmethod
    def try_decode(cls, raw: bytes) -> "CollectionDescriptor | None":
        """A descriptor if these bytes are one, else ``None`` — **without assuming JSON**.

        Discrimination has to work on opaque bytes, because a store holds images and
        model weights as readily as detections: parsing first would make ``read``
        unusable for exactly the payloads that most need the distributed form.

        A descriptor is always a JSON object written by this class, so anything not
        beginning with ``{`` is rejected on one byte — covering binary payloads and
        monolithic JSON lists alike. Sole occupancy of the marker matters too: a ref's
        output is arbitrary JSON the compiler never saw, so a payload that merely
        *contains* the marker alongside other fields is data, not a descriptor.
        """
        if raw.lstrip()[:1] != b"{":
            return None
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
        if not (isinstance(data, dict) and len(data) == 1 and _MARKER in data):
            return None
        return cls.decode(data)

    def flattened_with(
        self, others: Iterable["CollectionDescriptor"]
    ) -> "CollectionDescriptor":
        """Concatenate element lists. This is what makes a flattening merge cheap in
        distributed form: metadata only, no payload moved."""
        elements = list(self.elements)
        for other in others:
            elements.extend(other.elements)
        return CollectionDescriptor(tuple(elements), self.element_type)