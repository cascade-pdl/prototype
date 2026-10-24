"""Tests for ``cascade.deployment``.

The interchange property (a deployment's store block decodes through the
registry's *store-level* decode into a live store) is the invariant the whole
deployment -> engine -> node-env hop rests on, so it gets its own test.
"""
from cascade.deployment import Deployment
from cascade.store.file_store import FileConfig, FileStore
from cascade.store.s3_store import S3Config
from cascade.store.registry import decode as decode_store
import pytest

from cascade.deployment import DeploymentError
from cascade.model.resources import Resources
from cascade.substrate.backends import BackendKind, EcsConfig, LocalConfig


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


BACKENDS = [
    {"name": "local", "kind": "local", "max": {"cpu": 4, "memory": "8GiB"},
     "concurrency": 2, "config": {"aws_credentials_dir": "~/.aws", "no_pull": False}},
    {"name": "ecs-gpu", "kind": "ecs", "max": {"cpu": 32, "memory": "240GiB", "gpu": 4},
     "provides": ["www"], "concurrency": 4,
     "config": {"cluster": "c", "subnets": ["s1"], "capacity_provider": "gpu-asg"}},
]


def _raw(**extra):
    return {"store": {"kind": "s3", "config": {"bucket": "b"}}, "backends": BACKENDS, **extra}


def test_backends_decode_in_order_with_typed_config():
    dep = Deployment.decode(_raw())
    assert [b.name for b in dep.backends] == ["local", "ecs-gpu"]
    local, gpu = dep.backends
    assert local.kind is BackendKind.local and isinstance(local.config, LocalConfig)
    assert local.config.no_pull is False and local.config.map_current_user is True
    assert gpu.max == Resources(cpu=32, memory=240 * 1024, gpu=4)
    assert gpu.provides == {"www"}
    assert isinstance(gpu.config, EcsConfig) and gpu.config.capacity_provider == "gpu-asg"


def test_backends_round_trip():
    dep = Deployment.decode(_raw())
    assert Deployment.decode(dep.encode()).backends == dep.backends


def test_the_old_runners_block_is_refused_not_ignored():
    with pytest.raises(DeploymentError, match="backends"):
        Deployment.decode(_raw(runners={"docker": {"no_pull": True}}))


@pytest.mark.parametrize("change, message", [
    ({"max": None}, "needs `max"),
    ({"kind": "lambda"}, "kind"),
    ({"concurrency": None}, "needs `concurrency`"),
    ({"config": {"cluster": "c"}}, "subnets"),
    ({"config": {"cluster": "c", "subnets": ["s"], "launch_type": "FARGATE",
                 "capacity_provider": "x"}}, "not both"),
    ({"config": {"cluster": "c", "subnets": ["s"], "launch_type": "EC2"}}, "launch_type"),
    ({"surprise": 1}, "unknown"),
])
def test_an_unusable_backend_is_refused(change, message):
    ecs = {k: v for k, v in {**BACKENDS[1], **change}.items() if v is not None}
    with pytest.raises(DeploymentError, match=message):
        Deployment.decode(_raw(backends=[ecs]))


def test_backend_names_are_unique():
    with pytest.raises(DeploymentError, match="twice"):
        Deployment.decode(_raw(backends=[BACKENDS[0], BACKENDS[0]]))


def test_ecs_cannot_use_a_file_store():
    dep = Deployment.decode(
        {"store": {"kind": "file", "config": {"root": "/tmp/s"}}, "backends": BACKENDS}
    )
    assert len(dep.check()) == 1 and "ecs-gpu" in dep.check()[0]


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