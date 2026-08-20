"""``cascade.engine.runner.registry`` — RunConfig to live Runner.

The interesting behaviour is the three-leg merge: ``RefData`` is never overridden,
ref and deployment ``RunnerOverrides`` merge field-wise with the ref winning, and
machine-local context arrives separately in ``RunnerEnv``.
"""
from typing import Any

import pytest

from cascade.model.runner_kinds import RunnerKind
from cascade.model.ref_data import RefDocker, RefEcho, RefSubprocess
from cascade.model.runner_overrides import DockerOverride, RunnerOverrides
from cascade.plan.run_config import RunConfig
from cascade.protocol.run_spec import RunSpec
from cascade.plan.signature import Port, Signature, TypeExpr
from cascade.store.file_store import FileStore, FileConfig

from cascade.engine.runner.registry import (
    RunnerEnv,
    build_runner,
    merge_overrides,
    RUNNER_BUILDERS,
)
from cascade.engine.runner.runner_docker import RunnerDocker
from cascade.engine.runner.runner_echo import RunnerEcho
from cascade.engine.runner.runner_subprocess import RunnerSubprocess


def test_every_runner_kind_has_a_builder():
    """A registry that cannot cover its own vocabulary is incomplete — this is what
    was missing while RunnerKind.echo had no runner."""
    assert set(RUNNER_BUILDERS) == set(RunnerKind)


# --- dispatch ----------------------------------------------------------------

def test_builds_a_docker_runner():
    config = RunConfig(
        runner=RunnerKind.docker,
        config=RefDocker(image="flat-bug:v3", extra_args=["--gpus", "all"]),
    )
    runner = build_runner(config)
    assert isinstance(runner, RunnerDocker)
    assert runner.image == "flat-bug:v3"
    assert runner.extra_args == ["--gpus", "all"]


def test_builds_a_subprocess_runner():
    config = RunConfig(runner=RunnerKind.subprocess, config=RefSubprocess(cmd=["ls", "-l"]))
    runner = build_runner(config)
    assert isinstance(runner, RunnerSubprocess)
    assert runner.cmd == ["ls", "-l"]


def test_builds_an_echo_runner():
    config = RunConfig(runner=RunnerKind.echo, config=RefEcho(message="hi"))
    runner = build_runner(config)
    assert isinstance(runner, RunnerEcho)
    assert runner.message == "hi"


# --- override precedence -----------------------------------------------------

def test_merge_returns_the_other_when_one_is_absent():
    ref = DockerOverride(memory=1024)
    assert merge_overrides(ref, None) is ref
    assert merge_overrides(None, ref) is ref
    assert merge_overrides(None, None) is None


def test_ref_wins_where_it_states_a_value():
    merged = merge_overrides(
        DockerOverride(memory=8192),
        DockerOverride(memory=2048, cpu=2, no_pull=True),
    )
    assert merged.memory == 8192   # ref stated it
    assert merged.cpu == 2         # ref left it unset -> deployment default
    assert merged.no_pull is True


def test_deployment_supplies_defaults_through_to_the_runner():
    config = RunConfig(
        runner=RunnerKind.docker,
        config=RefDocker(image="x:1"),
        overrides=DockerOverride(memory=8192),
    )
    runner = build_runner(config, deployment_overrides=DockerOverride(memory=2048, cpu=4))
    assert runner.memory == 8192
    assert runner.cpu == 4


def test_merging_mismatched_override_types_is_an_error():

    class MockOverride(RunnerOverrides):

        def decode(cls, raw: dict[str, Any]):
            return cls()

        def encode(self) -> dict[str, Any]:
            return {}


    with pytest.raises(Exception):
        merge_overrides(DockerOverride(memory=1), MockOverride())


def test_docker_no_pull_defaults_to_true_when_unset():
    runner = build_runner(RunConfig(runner=RunnerKind.docker, config=RefDocker(image="x:1")))
    assert runner.no_pull is True


def test_no_pull_false_survives_the_merge():
    """A False must not be mistaken for 'unset' — only None means unset."""
    config = RunConfig(
        runner=RunnerKind.docker,
        config=RefDocker(image="x:1"),
        overrides=DockerOverride(no_pull=False),
    )
    assert build_runner(config).no_pull is False


# --- machine-local context ---------------------------------------------------

def test_env_supplies_credentials_and_user_mapping():
    config = RunConfig(runner=RunnerKind.docker, config=RefDocker(image="x:1"))
    runner = build_runner(
        config,
        env=RunnerEnv(aws_credentials_dir="~/.aws", map_current_user=False),
    )
    assert runner.aws_credentials_dir == "~/.aws"
    assert runner.map_current_user is False


def test_resource_limits_reach_the_docker_command():
    config = RunConfig(
        runner=RunnerKind.docker,
        config=RefDocker(image="x:1"),
        overrides=DockerOverride(memory=2048, cpu=2),
    )
    cmd = build_runner(config)._build_cmd(RunSpec(name="n", run_id="r"))
    assert "--memory" in cmd and "2048m" in cmd
    assert "--cpus" in cmd and "2" in cmd

# --- echo's output ports come from the signature -----------------------------

def test_echo_takes_its_output_ports_from_the_signature():
    """Per-runnable, like the runner itself — nothing here varies per instance."""
    sig = Signature(
        inputs={"d": Port(TypeExpr.parse("Detection"))},
        outputs={"scored": Port(TypeExpr.parse("Score")), "log": Port(TypeExpr.parse("string"))},
    )
    runner = build_runner(
        RunConfig(runner=RunnerKind.echo, config=RefEcho(message="hi")),
        signature=sig,
    )
    assert runner.outputs == {"scored": 0, "log": 0}


def test_echo_without_a_signature_writes_nothing():
    runner = build_runner(RunConfig(runner=RunnerKind.echo, config=RefEcho()))
    assert runner.outputs == {}


def test_other_kinds_ignore_the_signature():
    sig = Signature(inputs={}, outputs={"o": Port(TypeExpr.parse("string"))})
    runner = build_runner(
        RunConfig(runner=RunnerKind.docker, config=RefDocker(image="x:1")),
        signature=sig,
    )
    assert isinstance(runner, RunnerDocker)


@pytest.mark.asyncio
async def test_echo_writes_a_stub_for_each_output_port(tmp_path):
    """What makes a multi-node dag testable: a downstream node has something to read.
    Where the stubs land is decided by store_out's scope, not by echo."""
    store = FileStore(FileConfig(root=str(tmp_path), scope=("r1", "main", "d")))
    runner = build_runner(
        RunConfig(runner=RunnerKind.echo, config=RefEcho(message="detected")),
        signature=Signature(inputs={}, outputs={"dets": Port(TypeExpr.parse("Detection[]"))}),
    )
    rc = await runner.run(RunSpec(name="detect", run_id="r1", instance_id="r1/main/d", store_out=store))
    assert rc == 0
    written = store.get_json("dets")
    # Detection[] is depth 1, so the stub is a list -- a downstream node can fan it
    assert isinstance(written, list)
    assert written[0]["port"] == "dets"
    assert written[0]["echo"] == "detected"
    assert (tmp_path / "r1" / "main" / "d" / "dets").is_file()


@pytest.mark.asyncio
async def test_echo_is_harmless_without_a_writer_store():
    runner = build_runner(
        RunConfig(runner=RunnerKind.echo, config=RefEcho()),
        signature=Signature(inputs={}, outputs={"o": Port(TypeExpr.parse("string"))}),
    )
    assert await runner.run(RunSpec(name="n", run_id="r")) == 0

# --- docker must mount a file store -------------------------------------------

def _docker_for(spec_stores=True, tmp_path=None):
    from cascade.protocol.run_spec import RunSpec
    from cascade.store.file_store import FileConfig, FileStore

    runner = build_runner(RunConfig(runner=RunnerKind.docker, config=RefDocker(image="x:1")))
    if not spec_stores:
        return runner, RunSpec(name="n", run_id="r")
    base = FileConfig(root=str(tmp_path / "_store"), scope=("r1", "main"))
    spec = RunSpec(
        name="n", run_id="r1", instance_id="r1/main/n",
        store_in=FileStore(base), store_out=FileStore(base.subscope(("n",))),
    )
    return runner, spec


@pytest.mark.skip
def test_a_file_store_root_is_mounted_into_the_container(tmp_path):
    """Without this the container writes into its own filesystem and the data is
    discarded on exit — the run leaves only the plan the executor wrote host-side."""
    from cascade.engine.runner.runner_docker import CONTAINER_STORE_ROOT

    runner, spec = _docker_for(tmp_path=tmp_path)
    cmd = runner._build_cmd(spec)
    root = str((tmp_path / "_store").resolve())
    assert "-v" in cmd
    assert f"{root}:{CONTAINER_STORE_ROOT}" in cmd


@pytest.mark.skip
def test_the_container_side_is_a_posix_path_not_the_host_path(tmp_path):
    """A host path is not a valid mount target for a Linux container: `-v C:\\x:C:\\x`
    is meaningless on Windows. So the container side is a fixed POSIX path, and the store
    root in the spec is rewritten to match."""
    from cascade.engine.runner.runner_docker import CONTAINER_STORE_ROOT

    runner, spec = _docker_for(tmp_path=tmp_path)
    mount = [c for c in runner._build_cmd(spec) if c.endswith(CONTAINER_STORE_ROOT)][0]
    assert mount.endswith(f":{CONTAINER_STORE_ROOT}")
    assert CONTAINER_STORE_ROOT.startswith("/")


@pytest.mark.skip
def test_the_container_sees_the_rewritten_root(tmp_path):
    """The scope is untouched, so addressing still resolves — only the root changes."""
    import json

    from cascade.engine.runner.runner_docker import CONTAINER_STORE_ROOT
    from cascade.store.registry import decode as decode_store

    runner, spec = _docker_for(tmp_path=tmp_path)
    cmd = runner._build_cmd(spec)
    blob = [c.split("=", 1)[1] for c in cmd if c.startswith("CASCADE_STORE_OUT=")][0]
    store = decode_store(json.loads(blob))
    assert store.config.root == CONTAINER_STORE_ROOT
    assert store.config.scope == ("r1", "main", "n")  # unchanged


@pytest.mark.skip
def test_one_mount_even_though_there_are_two_stores(tmp_path):
    """Reader and writer are subscopes of one deployment store, so they share a root."""
    runner, spec = _docker_for(tmp_path=tmp_path)
    assert sum(1 for c in runner._build_cmd(spec) if c == "-v") == 1


@pytest.mark.skip
def test_no_mount_when_there_is_no_file_store():
    runner, spec = _docker_for(spec_stores=False)
    assert "-v" not in runner._build_cmd(spec)


def test_home_is_not_forced_when_no_credentials_are_mounted(tmp_path):
    """Forcing HOME breaks any image that pip-installed as its own user: the packages sit
    in that user's ~/.local, and a different HOME hides them."""
    runner, spec = _docker_for(tmp_path=tmp_path)
    assert not [c for c in runner._build_cmd(spec) if c.startswith("HOME=")]


def test_home_is_set_when_credentials_are_mounted(tmp_path):
    """boto looks for ~/.aws, so the mount and HOME have to agree — and that is the only
    reason to set it."""
    from cascade.engine.runner.registry import RunnerEnv
    from cascade.protocol.run_spec import RunSpec

    runner = build_runner(
        RunConfig(runner=RunnerKind.docker, config=RefDocker(image="x:1")),
        env=RunnerEnv(aws_credentials_dir=str(tmp_path / ".aws")),
    )
    cmd = runner._build_cmd(RunSpec(name="n", run_id="r"))
    home = [c.split("=", 1)[1] for c in cmd if c.startswith("HOME=")][0]
    assert any(c.endswith(f"{home}/.aws:ro") for c in cmd)