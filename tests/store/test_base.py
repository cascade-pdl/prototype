"""``cascade.store.base`` — the Store ABC's concrete contract.

Exercised through the in-memory double so no filesystem or AWS is involved. These
cover the behaviour every backend inherits and none of the backend-specific
tests touch: ``copy`` (same- and cross-store), the JSON helpers, and scope/``at``
composition.
"""
import pytest
from tests.support.memory_store import MemoryStore


@pytest.mark.asyncio
async def test_put_get_has_roundtrip(memory_store: MemoryStore):
    await memory_store.put("k", b"payload")
    assert await memory_store.has("k")
    assert await memory_store.get("k") == b"payload"
    assert not await memory_store.has("absent")


@pytest.mark.asyncio
async def test_json_helpers_roundtrip(memory_store: MemoryStore):
    obj = {"a": 1, "b": [2, 3]}
    await memory_store.put_json("doc", obj)
    assert await memory_store.get_json("doc") == obj


@pytest.mark.asyncio
async def test_at_fragments_partition_the_keyspace(memory_store: MemoryStore):
    await memory_store.put("out", b"1", at=("nodeA", "0"))
    await memory_store.put("out", b"2", at=("nodeA", "1"))
    assert await memory_store.get("out", at=("nodeA", "0")) == b"1"
    assert await memory_store.get("out", at=("nodeA", "1")) == b"2"
    assert not await memory_store.has("out")  # nothing at the bare root


@pytest.mark.asyncio
async def test_list_returns_keys_relative_to_the_scoped_base():
    store = MemoryStore(scope=("run7",))
    await store.put("a", b"x", at=("nodeA",))
    await store.put("b", b"y", at=("nodeA",))
    await store.put("c", b"z", at=("nodeB",))
    assert sorted(await store.list(at=("nodeA",))) == ["a", "b"]
    assert await store.list(at=("nodeB",)) == ["c"]


@pytest.mark.asyncio
async def test_scope_is_transparent_to_callers():
    scoped = MemoryStore(scope=("wilder", "moth"))
    await scoped.put("k", b"v")               # caller passes a bare logical key
    assert await scoped.get("k") == b"v"      # scope is the backend's business


@pytest.mark.asyncio
async def test_copy_within_a_store():
    store = MemoryStore()
    await store.put("src", b"data", at=("from",))
    await store.copy("src", "dst", from_=("from",), to_=("to",))
    assert await store.get("dst", at=("to",)) == b"data"


@pytest.mark.asyncio
async def test_copy_across_stores():
    a = MemoryStore()
    b = MemoryStore(scope=("dest",))
    await a.put("src", b"payload", at=("run",))
    await a.copy("src", "dst", from_=("run",), to_=("run",), dst_store=b)
    assert await b.get("dst", at=("run",)) == b"payload"
    assert not await a.has("dst", at=("run",))  # source store unchanged