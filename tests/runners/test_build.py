"""The ref runner builder — the seam between coordination and substrate.

``DagRunner`` used to carry a ``RunnerEnv`` and an overrides map, store both, thread both
through its recursion, and read neither. These pin that it no longer can.
"""
import inspect

import pytest
from yaml import safe_load

from cascade.engine.dag import DagRunner
from cascade.model.pipeline import Pipeline
from cascade.model.resources import Resources
from cascade.plan.compile import compile_pipeline
from cascade.runners import RunnerDocker, RunnerEcho
from cascade.runners.build import build_ref, configure_ref_builder
from cascade.substrate.backends import Backend, BackendKind, LocalConfig
from cascade.substrate.placement import Placement, PlacementError, PlacementMap


PIPELINE = """
entrypoint: main
input: []
types: { structures: [] }
refs:
  - { name: e, runner: echo, config: {}, input: [], output: [ { name: o, type: string } ] }
  - name: d
    runner: docker
    resources: { cpu: 1, memory: 1GiB }
    config: { image: "x:1", wrap: false }
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
    """What DagRunner hands the builder: the ref's name, its own config and its ports."""
    return name, plan.run_config[name], plan.signatures.get(name)


def _local(name="here", **config):
    return Backend(name=name, kind=BackendKind.local, max=Resources(8, 16384),
                   config=LocalConfig(**config))


def test_the_builder_turns_ref_data_into_a_runner(plan):
    build = configure_ref_builder()
    assert isinstance(build(*_ref(plan, "e")), RunnerEcho)
    assert isinstance(build(*_ref(plan, "d")), RunnerDocker)


def test_the_builder_never_sees_a_plan_or_a_deployment(plan):
    """Two stages: placement and backends bound once, ref data passed per call. The name
    is ref data: it is the key into the placement the executor already decided, so the
    builder reads a decision rather than making one."""
    assert list(inspect.signature(configure_ref_builder).parameters) == [
        "placements", "backends", "cwd"
    ]
    assert list(inspect.signature(build_ref).parameters) == [
        "name", "config", "signature", "placements", "backends", "cwd"
    ]


def test_a_ref_runs_on_the_backend_it_was_placed_on(plan):
    """Two backends of one kind, different settings: only the placement says which."""
    build = configure_ref_builder(
        placements=PlacementMap((Placement("d", "b"),)),
        backends=[_local("a", no_pull=True), _local("b", no_pull=False)],
    )
    runner = build(*_ref(plan, "d"))
    assert runner.no_pull is False
    assert (runner.cpu, runner.memory) == (1.0, 1024)   # sized by the ref, not the backend


def test_the_backend_supplies_the_machine_settings(plan):
    """They used to sit in a general-purpose context object threaded through coordination
    that read none of it; they describe the machine, so they live with its backend."""
    build = configure_ref_builder(
        placements=PlacementMap((Placement("d", "here"),)),
        backends=[_local(aws_credentials_dir="~/.aws", map_current_user=False)],
    )
    runner = build(*_ref(plan, "d"))
    assert runner.aws_credentials_dir == "~/.aws"
    assert runner.map_current_user is False


def test_an_unplaced_ref_is_an_error_not_a_guess(plan):
    build = configure_ref_builder(placements=PlacementMap(), backends=[_local()])
    with pytest.raises(PlacementError, match="not placed"):
        build(*_ref(plan, "d"))


def test_echo_is_never_placed(plan):
    build = configure_ref_builder(placements=PlacementMap(), backends=[])
    assert isinstance(build(*_ref(plan, "e")), RunnerEcho)


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
