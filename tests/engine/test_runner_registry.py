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
from cascade.engine.run_spec import RunSpec

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