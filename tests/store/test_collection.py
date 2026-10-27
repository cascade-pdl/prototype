"""``cascade.store.collection`` and the collection methods on ``Store``.

Collections are a store feature: a descriptor is a set of ``(scope, key)`` pairs, which
is store vocabulary throughout. ``read`` resolves the distributed form and passes the
monolithic form through, so a consumer makes one call for either.
"""
import json

import pytest

from cascade.store.collection import CollectionDescriptor, CollectionError
from cascade.store.file_store import FileConfig, FileStore


@pytest.fixture
def store(tmp_path) -> FileStore:
    return FileStore(FileConfig(root=str(tmp_path)))


# --- the two forms -----------------------------------------------------------

@pytest.mark.asyncio
async def test_reads_a_monolithic_collection(store):
    """For JSON a monolithic collection is already a list once parsed, which is why the
    codec never reached the critical path."""
    await store.put_json("numbers", [0, 1, 2])
    assert await store.read_json("numbers") == [0, 1, 2]
    assert await store.width("numbers") == 3
    assert not await store.is_collection("numbers")


@pytest.mark.asyncio
async def test_reads_a_distributed_collection(store):
    await store.put_json("item", "a", at=("part", "0"))
    await store.put_json("item", "b", at=("part", "1"))
    await store.write_collection("things", [(("part", "0"), "item"), (("part", "1"), "item")])

    assert await store.read_json("things") == ["a", "b"]
    assert await store.width("things") == 2
    assert await store.is_collection("things")


@pytest.mark.asyncio
async def test_a_scalar_reads_through_unchanged(store):
    await store.put_json("n", 7)
    assert await store.read_json("n") == 7


@pytest.mark.asyncio
async def test_elements_may_have_differing_keys(store):
    """Pairs, not a shared key: a flattening merge combines descriptors whose elements
    need not agree."""
    await store.put_json("left", 1, at=("a",))
    await store.put_json("right", 2, at=("b",))
    await store.write_collection("both", [(("a",), "left"), (("b",), "right")])
    assert await store.read_json("both") == [1, 2]


# --- recursion ---------------------------------------------------------------

@pytest.mark.asyncio
async def test_nested_collections_resolve_to_nested_lists(store):
    """A fan over a fan produces exactly this, so it must not come back as raw dicts."""
    for outer in range(2):
        for inner in range(2):
            await store.put_json("v", f"{outer}{inner}", at=(str(outer), str(inner)))
        # element scopes are relative to the descriptor, which sits at (outer,)
        await store.write_collection(
            "inner", [((str(i),), "v") for i in range(2)], at=(str(outer),)
        )
    await store.write_collection("outer", [((str(o),), "inner") for o in range(2)])

    assert await store.read_json("outer") == [["00", "01"], ["10", "11"]]


@pytest.mark.asyncio
async def test_a_descriptor_resolves_the_same_from_any_depth_above_it(store, tmp_path):
    """Position independence: element scopes are relative to the descriptor, so the
    engine can hand out stores at several depths and reads still agree."""
    from cascade.store.file_store import FileStore

    await store.put_json("v", 42, at=("node", "0"))
    await store.write_collection("out", [(("0",), "v")], at=("node",))

    deeper = FileStore(store.config.subscope(("node",)))
    assert await store.read_json("out", at=("node",)) == [42]
    assert await deeper.read_json("out") == [42]


@pytest.mark.asyncio
async def test_self_referential_descriptor_is_caught(store):
    """Unreachable through the engine, trivially hand-written, and unbounded recursion
    against a store is an unpleasant way to find out."""
    await store.write_collection("loop", [((), "loop")])
    with pytest.raises(CollectionError, match="nesting exceeded"):
        await store.read_json("loop")


# --- discrimination ---------------------------------------------------------

@pytest.mark.asyncio
async def test_the_marker_must_be_the_sole_top_level_key(store):
    """A ref's output is arbitrary JSON the compiler never saw; a payload that merely
    contains the marker alongside other fields is data, not a descriptor."""
    descriptor = CollectionDescriptor(elements=((("a",), "k"),)).encode()
    marker, body = next(iter(descriptor.items()))
    await store.put_json("data", {marker: body, "other": 1})
    assert not await store.is_collection("data")
    assert await store.read_json("data") == {marker: body, "other": 1}


@pytest.mark.asyncio
async def test_width_of_a_descriptor_does_not_read_the_elements(store):
    """The count is in the descriptor, so asking how wide a fan is stays cheap."""
    await store.write_collection("things", [(("nowhere", str(i)), "x") for i in range(500)])
    assert await store.width("things") == 500  # would raise if it resolved them


@pytest.mark.asyncio
async def test_width_of_a_non_collection_is_an_error(store):
    await store.put_json("scalar", 7)
    with pytest.raises(CollectionError):
        await store.width("scalar")


# --- the descriptor itself ---------------------------------------------------

def test_descriptor_round_trips():
    d = CollectionDescriptor(elements=((("a",), "k"), (("b",), "j")), element_type="Det")
    assert CollectionDescriptor.decode(d.encode()) == d
    assert d.count == 2
    # try_decode works on bytes, and folds the "is it one?" and "decode it" steps
    assert CollectionDescriptor.try_decode(json.dumps(d.encode()).encode()) == d
    assert CollectionDescriptor.try_decode(b"[1, 2]") is None
    assert CollectionDescriptor.try_decode(b'{"plain": "dict"}') is None
    assert CollectionDescriptor.try_decode(b"\x89PNG\r\n") is None


def test_flattening_concatenates_element_lists():
    """What makes a flattening merge cheap in distributed form: metadata only."""
    a = CollectionDescriptor(elements=((("0",), "v"),))
    b = CollectionDescriptor(elements=((("1",), "v"), (("2",), "v")))
    assert a.flattened_with([b]).count == 3


@pytest.mark.asyncio
async def test_every_backend_gets_the_collection_methods():
    """Concrete on the ABC over put/get, so the test double gets them free."""
    from tests.support.memory_store import MemoryStore

    store = MemoryStore()
    await store.put_json("v", 1, at=("0",))
    await store.write_collection("c", [(("0",), "v")])
    assert await store.read_json("c") == [1]
    assert await store.width("c") == 1


# --- binary payloads: read must not assume JSON ------------------------------

PNG = b"\x89PNG\r\n\x1a\n" + bytes(range(40))


@pytest.mark.asyncio
async def test_read_returns_bytes_for_a_binary_object(store):
    """The bug this guards: discriminating by parsing JSON first makes `read` unusable
    for images and model files — exactly the payloads the distributed form exists for."""
    await store.put("img", PNG)
    assert await store.read("img") == PNG


@pytest.mark.asyncio
async def test_read_resolves_a_collection_of_binary_objects(store):
    """A fan over images producing crops: N binary elements, one descriptor."""
    await store.put("img", PNG, at=("0",))
    await store.put("img", PNG[::-1], at=("1",))
    await store.write_collection("crops", [(("0",), "img"), (("1",), "img")])

    crops = await store.read("crops")
    assert crops == [PNG, PNG[::-1]]
    assert await store.width("crops") == 2
    assert await store.is_collection("crops")


@pytest.mark.asyncio
async def test_is_collection_is_safe_on_binary(store):
    await store.put("img", PNG)
    assert not await store.is_collection("img")


@pytest.mark.asyncio
async def test_width_of_opaque_bytes_is_a_clear_error(store):
    """A monolithic non-JSON collection cannot be measured without the port's declared
    encoding, which is the engine's knowledge, not the store's."""
    await store.put("blob", b"a,b,c\n1,2,3\n")
    with pytest.raises(CollectionError, match="opaque bytes"):
        await store.width("blob")


@pytest.mark.asyncio
async def test_read_json_and_read_agree_on_a_descriptor(store):
    await store.put_json("v", {"n": 1}, at=("0",))
    await store.write_collection("c", [(("0",), "v")])
    assert await store.read_json("c") == [{"n": 1}]
    assert await store.read("c") == [b'{"n": 1}']


@pytest.mark.asyncio
async def test_a_json_object_that_merely_contains_the_marker_is_data(store):
    """Sole-occupancy check, applied after parsing bytes rather than before."""
    from cascade.store.collection import CollectionDescriptor

    marker, body = next(iter(CollectionDescriptor(elements=((("a",), "k"),)).encode().items()))
    await store.put_json("data", {marker: body, "other": 1})
    assert not await store.is_collection("data")
    assert await store.read_json("data") == {marker: body, "other": 1}

@pytest.mark.asyncio
async def test_copy_resolved_carries_a_distributed_collection_with_its_elements(store):
    """The copy must read back the same through a store that cannot see the original:
    the elements travel too, and the new descriptor points at them."""
    await store.put_json("v", 1, at=("src", "0"))
    await store.put_json("v", 2, at=("src", "1"))
    await store.write_collection("v", [(("0",), "v"), (("1",), "v")], at=("src",))
    await store.write_collection("vv", [(("src",), "v")])

    dst = FileStore(store.config.subscope(("dst",)))
    await store.copy_resolved("vv", "copied", dst_store=dst)

    assert await dst.read_json("copied") == [[1, 2]]
    # nothing in the copy references outside its own scope
    await store.put_json("v", 99, at=("src", "0"))
    assert await dst.read_json("copied") == [[1, 2]]


@pytest.mark.asyncio
async def test_copy_resolved_copies_a_plain_object_as_is(store):
    await store.put("blob", b"\x89PNG", at=("a",))
    await store.copy_resolved("blob", "b", from_=("a",), to_=("b",))
    assert await store.get("b", at=("b",)) == b"\x89PNG"
