"""Tests for ``cascade.deployment``.

The interchange property (the store a deployment decodes to is the one that travels to
nodes in the registry's ``{kind, config}`` envelope) is the invariant the whole
deployment -> engine -> node-env hop rests on, so it gets its own test. The deployment
file's own block differs from that envelope on purpose: its ``scope`` sits beside
``kind``, not in ``config``.
"""
import copy

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
    assert back.store.endpoint is None


def test_an_s3_compatible_endpoint_round_trips():
    dep = Deployment(store=S3Config(bucket="b", region="garage", endpoint="http://localhost:3900"))
    assert Deployment.decode(dep.encode()).store.endpoint == "http://localhost:3900"


BACKENDS = [
    {"name": "local", "kind": "local", "max": {"cpu": 4, "memory": "8GiB"},
     "concurrency": 2, "config": {"no_pull": False}},
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


def test_the_old_credentials_mount_is_refused():
    """Ignoring it would lose the credentials silently and fail at the first store access."""
    raw = copy.deepcopy(_raw())
    raw["backends"][0]["config"]["aws_credentials_dir"] = "~/.aws"
    with pytest.raises(DeploymentError, match="credentials endpoint"):
        Deployment.decode(raw)


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


def test_the_decoded_store_is_what_travels_to_nodes(tmp_path):
    """Decoded, the deployment's scope is in the ``StoreConfig``, and the node envelope
    (``encode_config``, scope inside ``config``) rebuilds that same live store."""
    from cascade.store.registry import encode_config

    dep = Deployment.decode({
        "store": {"kind": "file", "scope": ["wilder", "moth"], "config": {"root": str(tmp_path)}},
    })
    envelope = encode_config(dep.store)
    assert envelope["config"]["scope"] == ["wilder", "moth"]
    live = decode_store(envelope)
    assert isinstance(live, FileStore)
    assert live.scope == ("wilder", "moth")


def test_the_store_block_carries_scope_beside_kind():
    dep = Deployment(store=FileConfig(root="./_store", scope=("team",)), project_scope=False)
    block = dep.encode()["store"]
    assert block == {
        "kind": "file", "scope": ["team"], "project_scope": False,
        "config": {"root": "./_store"},
    }
    assert Deployment.decode(dep.encode()).project_scope is False


def test_project_scope_is_on_by_default():
    dep = Deployment.decode({"store": {"kind": "file", "config": {"root": "./_store"}}})
    assert dep.project_scope is True
    assert dep.store.scope == ()


def test_a_scope_inside_config_is_refused_pointing_up():
    """One place to write it: a scope in both places could disagree."""
    with pytest.raises(DeploymentError, match="moved up.*beside `kind`"):
        Deployment.decode({"store": {"kind": "file", "config": {"root": ".", "scope": ["x"]}}})


@pytest.mark.parametrize("block, message", [
    ({"kind": "file", "config": {"root": "."}, "scope": "x"}, "list of names"),
    ({"kind": "file", "config": {"root": "."}, "project_scope": "no"}, "true or false"),
    ({"kind": "file", "config": {"root": "."}, "prefix": "x"}, "unknown store field"),
])
def test_a_malformed_store_block_is_named(block, message):
    with pytest.raises(DeploymentError, match=message):
        Deployment.decode({"store": block})


def test_named_limits_are_refused_pointing_at_the_pipeline():
    """Ignored, they would let the work run uncapped."""
    with pytest.raises(DeploymentError, match="moved to the pipeline"):
        Deployment.decode(_raw(limits={"heavy": 3}))
