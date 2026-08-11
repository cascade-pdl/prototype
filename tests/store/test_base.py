"""``cascade.store.base`` — the Store ABC's concrete contract.

Exercised through the in-memory double so no filesystem or AWS is involved. These
cover the behaviour every backend inherits and none of the backend-specific
tests touch: ``copy`` (same- and cross-store), the JSON helpers, and scope/``at``
composition.
"""
from tests.support.memory_store import MemoryStore


def test_put_get_has_roundtrip(memory_store: MemoryStore):
    memory_store.put("k", b"payload")
    assert memory_store.has("k")
    assert memory_store.get("k") == b"payload"
    assert not memory_store.has("absent")


def test_json_helpers_roundtrip(memory_store: MemoryStore):
    obj = {"a": 1, "b": [2, 3]}
    memory_store.put_json("doc", obj)
    assert memory_store.get_json("doc") == obj


def test_at_fragments_partition_the_keyspace(memory_store: MemoryStore):
    memory_store.put("out", b"1", at=("nodeA", "0"))
    memory_store.put("out", b"2", at=("nodeA", "1"))
    assert memory_store.get("out", at=("nodeA", "0")) == b"1"
    assert memory_store.get("out", at=("nodeA", "1")) == b"2"
    assert not memory_store.has("out")  # nothing at the bare root


def test_list_returns_keys_relative_to_the_scoped_base():
    store = MemoryStore(scope=("run7",))
    store.put("a", b"x", at=("nodeA",))
    store.put("b", b"y", at=("nodeA",))
    store.put("c", b"z", at=("nodeB",))
    assert sorted(store.list(at=("nodeA",))) == ["a", "b"]
    assert store.list(at=("nodeB",)) == ["c"]


def test_scope_is_transparent_to_callers():
    scoped = MemoryStore(scope=("wilder", "moth"))
    scoped.put("k", b"v")               # caller passes a bare logical key
    assert scoped.get("k") == b"v"      # scope is the backend's business


def test_copy_within_a_store():
    store = MemoryStore()
    store.put("src", b"data", at=("from",))
    store.copy("src", "dst", from_=("from",), to_=("to",))
    assert store.get("dst", at=("to",)) == b"data"


def test_copy_across_stores():
    a = MemoryStore()
    b = MemoryStore(scope=("dest",))
    a.put("src", b"payload", at=("run",))
    a.copy("src", "dst", from_=("run",), to_=("run",), dst_store=b)
    assert b.get("dst", at=("run",)) == b"payload"
    assert not a.has("dst", at=("run",))  # source store unchanged