"""A missing key looks the same from every backend: ``KeyNotFound`` from ``get``,
``False`` from ``has``. The S3 store runs against a stubbed client, so the error codes
are the ones botocore really produces for each call."""
import boto3
import pytest
from botocore.stub import Stubber

from cascade.store.base import KeyNotFound
from cascade.store.file_store import FileConfig, FileStore
from cascade.store.s3_store import S3Config, S3Store
from tests.support.memory_store import MemoryStore


def _s3() -> tuple[S3Store, Stubber]:
    store = S3Store(S3Config(bucket="b", prefix="p"))
    store._client = boto3.client(
        "s3",
        region_name="eu-west-1",
        aws_access_key_id="x",
        aws_secret_access_key="x",
    )
    return store, Stubber(store._client)


def test_file_store_get_missing(tmp_path):
    with pytest.raises(KeyNotFound):
        FileStore(FileConfig(root=str(tmp_path))).get("absent", at=("a", "b"))


def test_file_store_get_through_a_file(tmp_path):
    """A key whose 'directory' is itself a key: still just absent."""
    store = FileStore(FileConfig(root=str(tmp_path)))
    store.put("k", b"x")
    with pytest.raises(KeyNotFound):
        store.get("child", at=("k",))


def test_file_store_prefix_is_not_a_key(tmp_path):
    """S3 has no directories, so neither does a file store's contract."""
    store = FileStore(FileConfig(root=str(tmp_path)))
    store.put("leaf", b"x", at=("dir",))
    assert not store.has("dir")
    with pytest.raises(KeyNotFound):
        store.get("dir")


def test_file_store_construction_touches_nothing(tmp_path):
    """Review High #2: the docker runner builds a store rooted at the *container's*
    mount point; constructing it must not create that path on the host."""
    root = tmp_path / "nowhere"
    FileStore(FileConfig(root=str(root), scope=("a", "b")))
    assert not root.exists()


def test_file_store_put_makes_its_own_directories(tmp_path):
    store = FileStore(FileConfig(root=str(tmp_path / "fresh"), scope=("a",)))
    store.put("k", b"x", at=("b",))
    assert store.get("k", at=("b",)) == b"x"


def test_memory_store_get_missing():
    with pytest.raises(KeyNotFound):
        MemoryStore().get("absent")


def test_s3_get_missing():
    store, stub = _s3()
    stub.add_client_error(
        "get_object", service_error_code="NoSuchKey", http_status_code=404,
        expected_params={"Bucket": "b", "Key": "p/a/absent"},
    )
    with stub, pytest.raises(KeyNotFound, match="s3://b/p/a/absent"):
        store.get("absent", at=("a",))


def test_s3_has_missing():
    store, stub = _s3()
    stub.add_client_error("head_object", service_error_code="404", http_status_code=404)
    with stub:
        assert not store.has("absent")


def test_s3_copy_missing_source():
    store, stub = _s3()
    stub.add_client_error("copy_object", service_error_code="NoSuchKey", http_status_code=404)
    with stub, pytest.raises(KeyNotFound):
        store.copy("absent", "dst")


def test_s3_other_errors_are_not_masked():
    """Access denied is not 'absent': hiding it would turn a permissions problem into a
    confusing missing-input error."""
    from botocore.exceptions import ClientError

    store, stub = _s3()
    stub.add_client_error("get_object", service_error_code="AccessDenied", http_status_code=403)
    with stub, pytest.raises(ClientError):
        store.get("k")


def test_collection_with_a_missing_element(tmp_path):
    """Reads through a descriptor surface the same error as a direct read."""
    store = FileStore(FileConfig(root=str(tmp_path)))
    store.write_collection("c", [(("0",), "e")])
    with pytest.raises(KeyNotFound):
        store.read("c")
