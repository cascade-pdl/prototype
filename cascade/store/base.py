from __future__ import annotations

import asyncio
import json
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any, Callable, Self, Sequence, TypeVar

from cascade.store.collection import (
    MAX_DEPTH,
    CollectionDescriptor,
    CollectionError,
)


ELEMENTS = "$el"
"""Where ``copy_resolved`` puts a copied collection's elements. ``$`` marks an
engine-owned name, so no declared port can collide with it."""

MAX_IN_FLIGHT = 32
"""How many blocking store calls one process has in flight at once, across every store
instance and event loop in it. Each costs a round trip (on S3 from a laptop, ~140 ms a GET and
~240 ms a PUT), so overlapping them is where the time goes; bounding them keeps a
10,000-element collection from opening 10,000 connections."""

_CALLS = ThreadPoolExecutor(MAX_IN_FLIGHT, thread_name_prefix="cascade-store")
"""The threads blocking store calls run in. Its size is the bound: calls beyond it
queue. One pool for the process rather than one per loop or per store, so nested
concurrency (a collection's elements, inside lanes copying in parallel) cannot multiply
it. Store calls never wait on other store calls from inside a thread, so the queue
cannot deadlock."""

T = TypeVar("T")


async def blocking(fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
    """Run a blocking call in the store's threads, so the event loop keeps running."""
    return await asyncio.get_running_loop().run_in_executor(_CALLS, partial(fn, *args, **kwargs))


class KeyNotFound(LookupError):
    """``get`` of a key that is not in the store — the same exception from every backend.

    Without it a caller has to know which backend it holds: ``FileNotFoundError`` from a
    file store, a botocore ``ClientError`` from S3. Code that is backend-agnostic by
    design (the engine, the node SDK) could not handle a missing input at all.
    """


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

    **The API is async, and an async method never blocks.** The executor runs every
    dag, fan and lane on one event loop, so a store call that blocked it would stall
    all of them for a round trip. A backend with an async client implements the four
    primitives below directly; one whose client blocks subclasses
    :class:`BlockingStore`, which runs its synchronous primitives in the store's
    threads. Everything else here is built on the four.
    """

    config: StoreConfig

    @abstractmethod
    async def put(self, key: str, data: bytes, at: tuple[str, ...] = ()) -> str: ...

    @abstractmethod
    async def get(self, key: str, at: tuple[str, ...] = ()) -> bytes:
        """The bytes at ``key``; :class:`KeyNotFound` if there are none."""

    @abstractmethod
    async def has(self, key: str, at: tuple[str, ...] = ()) -> bool:
        """Whether ``get`` would succeed. A prefix that only has keys beneath it is not a
        key: S3 has no directories, so a file store must not report them either."""

    @abstractmethod
    async def list(self, at: tuple[str, ...] = ()) -> list[str]: ...

    # convenience JSON helpers
    async def put_json(self, key: str, obj: Any, at: tuple[str, ...] = ()) -> str:
        return await self.put(key, json.dumps(obj).encode("utf-8"), at=at)

    async def get_json(self, key: str, at: tuple[str, ...] = ()) -> Any:
        return json.loads((await self.get(key, at=at)).decode("utf-8"))

    async def copy(
        self,
        src_key: str,
        dst_key: str,
        from_: tuple[str, ...] = (),
        to_: tuple[str, ...] = (),
        dst_store: "Store | None" = None,
    ) -> str:
        """Copy a key to another store."""
        dst_store = dst_store or self
        return await dst_store.put(
            key=dst_key,
            data=await self.get(key=src_key, at=from_),
            at=to_,
        )

    async def copy_resolved(
        self,
        src_key: str,
        dst_key: str,
        from_: tuple[str, ...] = (),
        to_: tuple[str, ...] = (),
        dst_store: "Store | None" = None,
        _depth: int = 0,
    ) -> str:
        """Copy a key so the copy reads back the same from its new location.

        ``copy`` moves one object, which is wrong for a distributed collection: its
        descriptor's element scopes are relative to where it sits, and the elements may
        be somewhere the destination's readers cannot see. So the elements are copied
        too, beneath ``<to_>/$el/<dst_key>/<i>/``, and a new descriptor references them
        there. A collection of collections recurses.

        Every object passes through here, so the S3 ``copy`` fast path is not used: the
        descriptor check needs the bytes.
        """
        if _depth > MAX_DEPTH:
            raise CollectionError(
                f"collection nesting exceeded {MAX_DEPTH} at {'/'.join((*from_, src_key))}; "
                "a descriptor probably references itself"
            )
        dst_store = dst_store or self
        raw = await self.get(src_key, at=from_)
        descriptor = CollectionDescriptor.try_decode(raw)
        if descriptor is None:
            return await dst_store.put(dst_key, raw, at=to_)
        elements = [
            ((ELEMENTS, dst_key, str(index)), element_key)
            for index, (_scope, element_key) in enumerate(descriptor.elements)
        ]
        # the elements are independent: copy them all at once
        await asyncio.gather(*(
            self.copy_resolved(
                element_key,
                element_key,
                from_=(*from_, *scope),
                to_=(*to_, *element_scope),
                dst_store=dst_store,
                _depth=_depth + 1,
            )
            for (scope, element_key), (element_scope, _key) in zip(descriptor.elements, elements)
        ))
        return await dst_store.write_collection(
            dst_key, elements, at=to_, element_type=descriptor.element_type
        )

    # --- collections ------------------------------------------------------
    # One logical key, many stored objects. Concrete on the ABC and built only over
    # put/get, so every backend -- including the in-memory test double -- gets them.

    async def _read(
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
        raw = await self.get(key, at=at)
        if descriptor := CollectionDescriptor.try_decode(raw):
            # fetched all at once; gather keeps descriptor order whatever finishes first
            return list(await asyncio.gather(*(
                self._read(element_key, at=(*at, *scope), as_json=as_json, depth=depth + 1)
                for scope, element_key in descriptor.elements
            )))
        return json.loads(raw.decode("utf-8")) if as_json else raw

    async def read(self, key: str, at: tuple[str, ...] = ()) -> bytes | list:
        return await self._read(key, at=at, as_json=False, depth=0)

    async def read_json(self, key: str, at: tuple[str, ...] = ()) -> Any:
        return await self._read(key, at=at, as_json=True, depth=0)

    async def write_collection(
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
        return await self.put_json(key, descriptor.encode(), at=at)

    async def width(self, key: str, at: tuple[str, ...] = ()) -> int:
        raw = await self.get(key, at=at)
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

    async def is_collection(self, key: str, at: tuple[str, ...] = ()) -> bool:
        return CollectionDescriptor.try_decode(await self.get(key, at=at)) is not None


class BlockingStore(Store):
    """A store whose client blocks (files, boto3): it implements synchronous primitives,
    and the async methods run them in the store's threads (:func:`blocking`), so the
    backend cannot block the event loop by mistake. The threads are the process's
    bound on blocking calls in flight (:data:`MAX_IN_FLIGHT`)."""

    async def put(self, key: str, data: bytes, at: tuple[str, ...] = ()) -> str:
        return await blocking(self._put, key, data, at)

    async def get(self, key: str, at: tuple[str, ...] = ()) -> bytes:
        return await blocking(self._get, key, at)

    async def has(self, key: str, at: tuple[str, ...] = ()) -> bool:
        return await blocking(self._has, key, at)

    async def list(self, at: tuple[str, ...] = ()) -> list[str]:
        return await blocking(self._list, at)

    @abstractmethod
    def _put(self, key: str, data: bytes, at: tuple[str, ...]) -> str: ...

    @abstractmethod
    def _get(self, key: str, at: tuple[str, ...]) -> bytes: ...

    @abstractmethod
    def _has(self, key: str, at: tuple[str, ...]) -> bool: ...

    @abstractmethod
    def _list(self, at: tuple[str, ...]) -> list[str]: ...
