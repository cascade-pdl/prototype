"""The store's async promise: a call never blocks the event loop, a collection's elements
are fetched at once and come back in order, and the process has a bounded number of
calls in flight. Backends are made slow by sleeping inside their blocking primitives,
which is exactly where a real round trip waits."""
import asyncio
import threading
import time

import pytest

from cascade.node.node import Node, NodeError
from cascade.store import base
from cascade.store.base import KeyNotFound, Store
from cascade.store.file_store import FileConfig, FileStore
from cascade.store.s3_store import S3Config, S3Store, _shared_client
from tests.support.memory_store import MemoryStore

SLOW = 0.2


async def _ticks_during(call) -> int:
    """How often a 10 ms ticker ran while ``call`` was awaited."""
    ticks = 0
    done = asyncio.Event()

    async def ticker():
        nonlocal ticks
        while not done.is_set():
            await asyncio.sleep(0.01)
            ticks += 1

    task = asyncio.create_task(ticker())
    try:
        await call
    finally:
        done.set()
        await task
    return ticks


class _SlowClient:
    """Stands in for a boto client whose every call takes a round trip."""

    def get_object(self, **_kwargs):
        time.sleep(SLOW)

        class Body:
            def read(self):
                return b"1"

        return {"Body": Body()}

    def copy_object(self, **_kwargs):
        time.sleep(SLOW)


def _slow_file_store(tmp_path, monkeypatch) -> FileStore:
    original = FileStore._get

    def slow(self, key, at=()):
        time.sleep(SLOW)
        return original(self, key, at)

    monkeypatch.setattr(FileStore, "_get", slow)
    store = FileStore(FileConfig(root=str(tmp_path)))
    (tmp_path / "k").write_bytes(b"1")
    return store


def _slow_s3_store() -> S3Store:
    store = S3Store(S3Config(bucket="b"))
    store._client = _SlowClient()
    return store


@pytest.mark.asyncio
async def test_a_slow_file_store_call_does_not_block_the_loop(tmp_path, monkeypatch):
    store = _slow_file_store(tmp_path, monkeypatch)
    assert await _ticks_during(store.get("k")) >= 10


@pytest.mark.asyncio
async def test_a_slow_s3_call_does_not_block_the_loop():
    assert await _ticks_during(_slow_s3_store().get("k")) >= 10


@pytest.mark.asyncio
async def test_the_s3_server_side_copy_does_not_block_the_loop():
    store = _slow_s3_store()
    assert await _ticks_during(store.copy("a", "b")) >= 10


class _Recording(MemoryStore):
    """Counts calls in flight at once; earlier keys are slower, so completion order is
    the reverse of descriptor order."""

    def __init__(self):
        super().__init__()
        self.live = 0
        self.peak = 0
        self.lock = threading.Lock()

    def _get(self, key, at=()):
        with self.lock:
            self.live += 1
            self.peak = max(self.peak, self.live)
        try:
            if at and at[-1].isdigit():
                time.sleep(0.002 * (100 - int(at[-1])))
            return super()._get(key, at)
        finally:
            with self.lock:
                self.live -= 1


async def _collection(store, width: int) -> None:
    for i in range(width):
        await store.put_json("v", i, at=("e", str(i)))
    await store.write_collection("c", [(("e", str(i)), "v") for i in range(width)])


@pytest.mark.asyncio
async def test_a_collection_is_read_at_once_and_in_order():
    store = _Recording()
    await _collection(store, 100)
    started = time.monotonic()
    assert await store.read_json("c") == list(range(100))
    # one at a time would take ~10 s (100 elements, 2-200 ms each)
    assert time.monotonic() - started < 3
    assert 1 < store.peak <= base.MAX_IN_FLIGHT


@pytest.mark.asyncio
async def test_a_copied_collection_keeps_its_order():
    src, dst = _Recording(), MemoryStore()
    await _collection(src, 40)
    await src.copy_resolved("c", "c", dst_store=dst)
    assert await dst.read_json("c") == list(range(40))
    assert src.peak > 1


class _Counted(MemoryStore):
    """Every instance counts into the same tally: what is bounded is the process."""

    live = 0
    peak = 0
    lock = threading.Lock()

    def _get(self, key, at=()):
        cls = type(self)
        with cls.lock:
            cls.live += 1
            cls.peak = max(cls.peak, cls.live)
        try:
            time.sleep(0.01)
            return super()._get(key, at)
        finally:
            with cls.lock:
                cls.live -= 1


@pytest.mark.asyncio
async def test_calls_in_flight_are_bounded_across_stores():
    """Two stores reading wide collections at once share one bound, not one each."""
    a, b = _Counted(), _Counted()
    await _collection(a, 100)
    await _collection(b, 100)
    await asyncio.gather(a.read_json("c"), b.read_json("c"))
    assert _Counted.peak == base.MAX_IN_FLIGHT


class _NativeAsync(Store):
    """A backend with an async client of its own: only the four async primitives, no
    synchronous ones, no threads."""

    def __init__(self):
        self.data: dict[str, bytes] = {}

    @staticmethod
    def _key(key, at):
        return "/".join((*at, key))

    async def put(self, key, data, at=()):
        await asyncio.sleep(SLOW / 10)  # the client's own non-blocking wait
        self.data[self._key(key, at)] = data
        return key

    async def get(self, key, at=()):
        await asyncio.sleep(SLOW / 10)
        try:
            return self.data[self._key(key, at)]
        except KeyError:
            raise KeyNotFound(key) from None

    async def has(self, key, at=()):
        return self._key(key, at) in self.data

    async def list(self, at=()):
        return sorted(self.data)


@pytest.mark.asyncio
async def test_a_natively_async_backend_needs_only_the_async_primitives():
    """The collection helpers work over it unchanged, and it does not block the loop."""
    src, dst = _NativeAsync(), _NativeAsync()
    await _collection(src, 20)
    assert await src.read_json("c") == list(range(20))
    await src.copy_resolved("c", "c", dst_store=dst)
    assert await dst.read_json("c") == list(range(20))
    assert await _ticks_during(src.read_json("c")) >= 1


@pytest.mark.asyncio
async def test_a_missing_element_raises_key_not_found():
    store = MemoryStore()
    await store.put_json("v", 0, at=("e", "0"))
    await store.write_collection("c", [(("e", "0"), "v"), (("e", "1"), "v")])
    with pytest.raises(KeyNotFound):
        await store.read_json("c")


def test_s3_stores_share_a_client_per_region_and_endpoint(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "x")
    one = S3Store(S3Config(bucket="b", region="eu-west-1", scope=("a",)))
    two = S3Store(S3Config(bucket="b", region="eu-west-1", scope=("b",)))
    other = S3Store(S3Config(bucket="b", region="eu-west-1", endpoint="http://localhost:3900"))
    assert one.client is two.client
    assert other.client is not one.client
    # another identity in the environment is another client, signing as that identity
    # (boto3's default session would have kept the first keys it saw)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "y")
    again = _shared_client("eu-west-1", None)
    assert again is not one.client
    assert again._request_signer._credentials.access_key == "y"


@pytest.mark.asyncio
async def test_the_node_sdk_refuses_to_run_inside_an_event_loop(tmp_path):
    """One loop cannot run inside another: the sync methods say so, rather than
    blocking or failing obscurely, and point at the stores."""
    store = FileStore(FileConfig(root=str(tmp_path)))
    node = Node(store_out=store)
    with pytest.raises(NodeError, match="await the store directly"):
        node._sync(store.put("k", b"1"))


def test_the_node_sdk_closes_its_loop_on_exit(tmp_path):
    store = FileStore(FileConfig(root=str(tmp_path)))
    with Node(store_out=store) as node:
        node._sync(store.put("k", b"1"))
        loop = node._loop
        assert loop is not None
    assert node._loop is None
    with pytest.raises(RuntimeError, match="closed"):
        loop.get_loop()
