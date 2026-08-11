"""Tests for ``cascade.deployment``.

The interchange property (a deployment's store block decodes through the
registry's *store-level* decode into a live store) is the invariant the whole
deployment -> engine -> node-env hop rests on, so it gets its own test.
"""
from cascade.deployment import Deployment
from cascade.store.file_store import FileConfig, FileStore
from cascade.store.s3_store import S3Config, S3Store
from cascade.store.registry import decode as decode_store
from cascade.model.runner_kinds import RunnerKind
from cascade.model.runner_overrides import DockerOverride


def test_file_store_round_trip_preserves_scope():
    dep = Deployment(name="local", store=FileConfig(root="./_store", scope=("wilder", "moth")))
    back = Deployment.decode(dep.encode())
    assert isinstance(back.store, FileConfig)
    assert back.store.root == "./_store"
    assert back.store.scope == ("wilder", "moth")
    assert back.name == "local"


def test_s3_store_round_trip():
    dep = Deployment(store=S3Config(bucket="wilder-data", prefix="moth", region="eu-west-1"))
    back = Deployment.decode(dep.encode())
    assert isinstance(back.store, S3Config)
    assert back.store.bucket == "wilder-data"
    assert back.store.prefix == "moth"
    assert back.store.region == "eu-west-1"


def test_runner_overrides_parse_by_kind():
    dep = Deployment(
        store=FileConfig(root="/tmp/s"),
        runners={RunnerKind.docker: DockerOverride(no_pull=True, memory=2048)},
    )
    back = Deployment.decode(dep.encode())
    assert back.runners[RunnerKind.docker].no_pull is True
    assert back.runners[RunnerKind.docker].memory == 2048


def test_load_from_explicit_path(tmp_path):
    dep = Deployment(name="local", store=FileConfig(root="./_store", scope=("run",)))
    path = tmp_path / "deployment.yaml"
    dep.save(path)
    loaded = Deployment.load(path)
    assert loaded.name == "local"
    assert loaded.store.scope == ("run",)


def test_store_block_is_interchangeable_with_env_path(tmp_path):
    # the block a deployment emits (config-level encode) must decode through the
    # store-level `decode` — the same call the node env path uses — into a live store
    dep = Deployment(store=FileConfig(root=str(tmp_path), scope=("wilder", "moth")))
    store_block = dep.encode()["store"]
    live = decode_store(store_block)
    assert isinstance(live, FileStore)
    assert live.scope == ("wilder", "moth")