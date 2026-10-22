"""The ref runner builder — the seam between coordination and substrate.

``DagRunner`` used to carry a ``RunnerEnv`` and an overrides map, store both, thread both
through its recursion, and read neither. These pin that it no longer can.
"""
import inspect

import pytest
from yaml import safe_load

from cascade.engine.dag import DagRunner
from cascade.model.pipeline import Pipeline
from cascade.model.runner_kinds import RunnerKind
from cascade.model.runner_overrides import DockerOverride
from cascade.plan.compile import compile_pipeline
from cascade.runners import RunnerDocker, RunnerEcho
from cascade.runners.build import build_ref, configure_ref_builder


PIPELINE = """
entrypoint: main
input: []
types: { structures: [] }
refs:
  - { name: e, runner: echo, config: {}, input: [], output: [ { name: o, type: string } ] }
  - name: d
    runner: docker
    config: { image: "x:1" }
    input:  [ { name: i, type: string } ]
    output: [ { name: o2, type: string } ]
dags:
  - name: inner
    input: []
    nodes: [ { name: a, runs: e } ]
    output: [ { node: a, field: o, as: out } ]
  - name: main
    input: []
    nodes:
      - { name: a, runs: e }
      - { name: b, runs: d, depends_on: [ { node: a, field: o, as: i } ] }
      - { name: c, runs: inner }
    output: [ { node: b, field: o2, as: out } ]
"""


@pytest.fixture
def plan():
    return compile_pipeline(Pipeline.decode(safe_load(PIPELINE)))


def _ref(plan, name):
    """What DagRunner hands the builder: the ref's own config and its resolved ports."""
    return plan.run_config[name], plan.signatures.get(name)


def test_the_builder_turns_ref_data_into_a_runner(plan):
    build = configure_ref_builder()
    assert isinstance(build(*_ref(plan, "e")), RunnerEcho)
    assert isinstance(build(*_ref(plan, "d")), RunnerDocker)


def test_the_builder_never_sees_a_plan_or_a_name(plan):
    """Two stages: deployment data bound once, ref data passed per call. The closure
    carries configuration and nothing about any particular pipeline."""
    assert list(inspect.signature(configure_ref_builder).parameters) == ["overrides", "cwd"]
    assert list(inspect.signature(build_ref).parameters) == [
        "config", "signature", "overrides", "cwd"
    ]


def test_deployment_overrides_reach_the_runner(plan):
    build = configure_ref_builder(
        overrides={RunnerKind.docker: DockerOverride(memory=4096, no_pull=False)}
    )
    runner = build(*_ref(plan, "d"))
    assert (runner.memory, runner.no_pull) == (4096, False)


def test_dockers_own_settings_travel_as_overrides(plan):
    """They used to sit in a general-purpose context object threaded through coordination
    that read none of it; they are docker settings, so they live with docker's."""
    build = configure_ref_builder(
        overrides={
            RunnerKind.docker: DockerOverride(
                aws_credentials_dir="~/.aws", map_current_user=False
            )
        }
    )
    runner = build(*_ref(plan, "d"))
    assert runner.aws_credentials_dir == "~/.aws"
    assert runner.map_current_user is False


def test_cwd_is_bound_for_the_run_not_passed_per_ref(plan):
    """Where a ref's *relative* command resolves is a property of which document was run:
    invocation context, so it is *not* in a deployment file — but it is fixed for the run,
    so it belongs on the bound side of the closure rather than in ref data."""
    assert "cwd" in inspect.signature(configure_ref_builder).parameters
    assert configure_ref_builder(cwd="/elsewhere")(*_ref(plan, "e"))


# --- what the injection removed from DagRunner --------------------------------

def test_dag_runner_takes_only_the_plan_and_a_builder():
    parameters = list(inspect.signature(DagRunner.__init__).parameters)
    assert parameters == ["self", "dag", "plan", "build_ref"]


def test_dag_runner_carries_no_configuration(plan):
    runner = DagRunner("main", plan)
    assert not hasattr(runner, "env")
    assert not hasattr(runner, "overrides")


def test_the_builder_is_passed_down_the_recursion(plan):
    """A nested dag must build its refs the same way its parent does, or a subdag would
    silently ignore the deployment."""
    sentinel = configure_ref_builder()
    child = DagRunner("main", plan, sentinel)._runner_for("inner")
    assert isinstance(child, DagRunner)
    assert child.build_ref is sentinel


def test_a_child_dag_gets_only_its_own_slice(plan):
    """Encapsulation enforced rather than promised: a subdag cannot reach a sibling's
    config because the entry is not there. It is also the same projection a
    dag-in-a-container must ship, so local recursion and remote dispatch stay one
    operation instead of two paths."""
    child = DagRunner("main", plan, configure_ref_builder())._runner_for("inner")
    assert set(child.plan.node_graphs) == {"inner"}
    assert set(child.plan.run_config) == {"e"}   # not "d", which only `main` uses
    assert child.plan.entrypoint == "inner"      # the slice is self-describing
