"""``Store.check``: before a run starts anything, a store that cannot be used fails in
one line saying what to fix. S3 runs against a stubbed client, so the errors are the ones
botocore really produces."""
import os

import boto3
import pytest
from botocore.stub import Stubber

from cascade.store.base import StoreError
from cascade.store.file_store import FileConfig, FileStore
from cascade.store.s3_store import S3Config, S3Store
from tests.support.memory_store import MemoryStore


@pytest.fixture
def no_aws_identity(monkeypatch, tmp_path):
    """An environment in which boto finds no credentials anywhere, without waiting on
    the instance metadata service."""
    for name in list(os.environ):
        if name.startswith("AWS_"):
            monkeypatch.delenv(name)
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "none"))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "none"))
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")


@pytest.fixture
def aws_identity(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "x")


def _s3() -> tuple[S3Store, Stubber]:
    store = S3Store(S3Config(bucket="b", region="eu-west-1"))
    store._client = boto3.client(
        "s3", region_name="eu-west-1", aws_access_key_id="x", aws_secret_access_key="x"
    )
    return store, Stubber(store._client)


@pytest.mark.asyncio
async def test_no_credentials_says_how_to_provide_them(no_aws_identity):
    store, stub = _s3()
    with stub, pytest.raises(StoreError, match="no AWS credentials.*AWS_PROFILE"):
        await store.check()  # no request made: the stub has none queued


@pytest.mark.asyncio
async def test_a_reachable_bucket_passes(aws_identity):
    store, stub = _s3()
    stub.add_response("head_bucket", {}, {"Bucket": "b"})
    with stub:
        await store.check()


@pytest.mark.asyncio
@pytest.mark.parametrize("code, status, message", [
    ("404", 404, "no such bucket"),
    ("403", 403, "access denied"),
    ("301", 301, "the bucket is in another region"),
])
async def test_a_bucket_that_cannot_be_used_is_named(aws_identity, code, status, message):
    store, stub = _s3()
    stub.add_client_error("head_bucket", service_error_code=code, http_status_code=status)
    with stub, pytest.raises(StoreError, match=f"s3://b: {message}"):
        await store.check()


@pytest.mark.asyncio
async def test_a_file_store_root_not_yet_made_passes(tmp_path):
    await FileStore(FileConfig(root=str(tmp_path / "not" / "yet"))).check()


@pytest.mark.asyncio
@pytest.mark.skipif(os.geteuid() == 0, reason="root can write anywhere")
async def test_an_unwritable_file_store_root_is_named(tmp_path):
    locked = tmp_path / "locked"
    locked.mkdir()
    locked.chmod(0o500)
    try:
        with pytest.raises(StoreError, match="not writable"):
            await FileStore(FileConfig(root=str(locked / "store"))).check()
    finally:
        locked.chmod(0o700)


@pytest.mark.asyncio
async def test_a_file_where_the_root_should_be_is_named(tmp_path):
    (tmp_path / "file").write_text("")
    with pytest.raises(StoreError, match="not a directory"):
        await FileStore(FileConfig(root=str(tmp_path / "file" / "store"))).check()


@pytest.mark.asyncio
async def test_a_store_with_nothing_to_check_passes():
    await MemoryStore().check()
