"""Placement: first fit over the deployment's ordered backends."""
import pytest
from yaml import safe_load

from cascade.deployment import Deployment
from cascade.executor import Executor, ExecutorError, place
from cascade.model.pipeline import Pipeline
from cascade.model.resources import Resources
from cascade.plan.compile import compile_pipeline
from cascade.substrate.backends import Backend, BackendKind, EcsConfig, LocalConfig
from cascade.substrate.placement import (
    Placement, PlacementError, PlacementMap, decide, place_refs,
)
from tests.support.memory_store import MemoryStore


PIPELINE = """
entrypoint: main
input: []
types: { structures: [] }
refs:
  - { name: tidy, runner: subprocess, config: { cmd: ["true"] },
      resources: { cpu: 1, memory: 2GiB }, input: [], output: [ { name: o, type: string } ] }
  - { name: classify, runner: docker, config: { image: "c:1", wrap: false },
      resources: { cpu: 4, memory: 32GiB }, input: [], output: [ { name: o, type: string } ] }
  - { name: flat-bug, runner: docker, config: { image: "f:1", wrap: false },
      resources: { cpu: 4, memory: 16GiB, gpu: 1 }, requires: [www],
      input: [], output: [ { name: o, type: string } ] }
  - { name: stub, runner: echo, config: {}, input: [], output: [ { name: o, type: string } ] }
dags:
  - name: main
    input: []
    nodes:
      - { name: a, runs: tidy }
      - { name: b, runs: classify }
      - { name: c, runs: flat-bug }
      - { name: d, runs: stub }
    output: [ { node: a, field: o, as: out } ]
"""


@pytest.fixture
def plan():
    return compile_pipeline(Pipeline.decode(safe_load(PIPELINE)))


def local(name="local", cpu=8, memory=16384, provides=()):
    return Backend(name=name, kind=BackendKind.local, max=Resources(cpu, memory),
                   config=LocalConfig(), provides=frozenset(provides))


def ecs(name, cpu=16, memory=122880, gpu=0, provides=("www",)):
    return Backend(name=name, kind=BackendKind.ecs, max=Resources(cpu, memory, gpu),
                   config=EcsConfig(cluster="c", subnets=("s",)),
                   provides=frozenset(provides), concurrency=4)


BACKENDS = [local(), ecs("ecs-cpu"), ecs("ecs-gpu", cpu=32, memory=245760, gpu=4)]


def test_first_fit_in_deployment_order(plan):
    assert place_refs(plan, BACKENDS) == PlacementMap((
        Placement("tidy", "local"),         # fits the first backend
        Placement("classify", "ecs-cpu"),   # too big for local
        Placement("flat-bug", "ecs-gpu"),   # needs a gpu and www
    ))


def test_order_is_the_priority(plan):
    """The same backends, expensive one first: every docker ref now goes there."""
    placed = place_refs(plan, [ecs("ecs-gpu", cpu=32, memory=245760, gpu=4), local()])
    assert placed.backend_of("classify") == placed.backend_of("flat-bug") == "ecs-gpu"
    assert placed.backend_of("tidy") == "local"   # ECS cannot run a subprocess


def test_echo_is_never_placed(plan):
    assert "stub" not in {p.ref for p in place_refs(plan, BACKENDS)}


def test_a_subprocess_only_runs_locally(plan):
    """A subprocess is a command on the executor's host; ECS cannot run one."""
    (tidy,) = [d for d in decide(plan, BACKENDS[1:]) if d.ref == "tidy"]
    assert tidy.backend is None
    assert tidy.skipped["ecs-cpu"] == ["a ecs backend cannot run a subprocess ref"]


def test_requires_must_be_provided(plan):
    (fb,) = [d for d in decide(plan, [ecs("gpu-private", gpu=4, provides=())]) if d.ref == "flat-bug"]
    assert fb.backend is None
    assert fb.skipped["gpu-private"] == ["does not provide ['www']"]


def test_every_unplaceable_ref_is_reported_together(plan):
    with pytest.raises(PlacementError) as e:
        place_refs(plan, [local()])
    assert len(e.value.errors) == 2
    assert all("fits no backend" in m for m in e.value.errors)


def test_no_backends_says_so(plan):
    with pytest.raises(PlacementError, match="lists no backends"):
        place_refs(plan, [])


def test_the_map_round_trips_and_refuses_duplicates():
    m = PlacementMap((Placement("a", "x"), Placement("b", "y")))
    assert PlacementMap.decode(m.encode()) == m
    assert m.backend_of("b") == "y" and m.backend_of("c") is None
    with pytest.raises(PlacementError, match="twice"):
        PlacementMap((Placement("a", "x"), Placement("a", "y")))


# --- the executor places before anything runs --------------------------------

def _deployment(store_kind="s3", backends=BACKENDS):
    store = {"kind": "s3", "config": {"bucket": "b"}} if store_kind == "s3" else \
        {"kind": "file", "config": {"root": "/nowhere"}}
    return Deployment.decode({"store": store, "backends": [b.encode() for b in backends]})


def test_place_adds_deployment_wide_problems(plan):
    with pytest.raises(PlacementError) as e:
        place(plan, _deployment("file", [local()]))
    # two refs that fit nowhere; no ecs backend, so no store complaint
    assert len(e.value.errors) == 2
    with pytest.raises(PlacementError, match="file store"):
        place(plan, _deployment("file"))


def test_an_unplaceable_run_fails_at_construction(plan):
    with pytest.raises(ExecutorError, match="cannot place"):
        Executor(plan, deployment=_deployment(backends=[local()]), store=MemoryStore())


def test_no_deployment_runs_everything_locally(plan):
    executor = Executor(plan, store=MemoryStore())
    assert {p.backend for p in executor.placements} == {"local"}


# --- the bundle a wrapped docker ref needs ------------------------------------

WRAPPED = """
entrypoint: main
input: []
types: { structures: [] }
refs:
  - { name: raw, runner: docker, config: { image: "python:3.13-slim" },
      resources: { cpu: 1, memory: 1GiB }, input: [], output: [ { name: o, type: string } ] }
  - { name: sdk, runner: docker, config: { image: "mine:1", wrap: false },
      resources: { cpu: 1, memory: 1GiB }, input: [], output: [ { name: o, type: string } ] }
dags:
  - name: main
    input: []
    nodes: [ { name: a, runs: raw }, { name: b, runs: sdk } ]
    output: [ { node: a, field: o, as: out } ]
"""


@pytest.fixture
def wrapped_plan():
    return compile_pipeline(Pipeline.decode(safe_load(WRAPPED)))


def with_bundle(name="local", bundle="dist/bundle/opt-cascade"):
    return Backend(name=name, kind=BackendKind.local, max=Resources(8, 16384),
                   config=LocalConfig(bundle=bundle), provides=frozenset())


def test_a_wrapped_docker_ref_on_a_local_backend_without_a_bundle_is_refused(wrapped_plan):
    """Found before anything runs, naming the wrapped refs only: ``sdk`` speaks the
    protocol itself and needs no bundle."""
    with pytest.raises(PlacementError) as e:
        place(wrapped_plan, _deployment("file", [local()]))
    [error] = e.value.errors
    assert "['raw']" in error and "no `bundle`" in error


def test_a_local_backend_with_a_bundle_runs_wrapped_refs(wrapped_plan):
    placed = place(wrapped_plan, _deployment("file", [with_bundle()]))
    assert placed.backend_of("raw") == placed.backend_of("sdk") == "local"


def test_no_deployment_has_no_bundle_either(wrapped_plan):
    with pytest.raises(PlacementError, match="no `bundle`"):
        place(wrapped_plan, None)


def test_an_ecs_backend_is_not_asked_for_a_local_bundle(wrapped_plan):
    """On ECS the bundle is appended to the image, not mounted from this host."""
    placed = place(wrapped_plan, _deployment("s3", [ecs("ecs-cpu")]))
    assert placed.backend_of("raw") == "ecs-cpu"


# --- named limits ----------------------------------------------------------------

def test_a_limit_the_deployment_does_not_define_is_refused_before_the_run():
    """Not "unlimited": a cap that silently stops applying floods what it protected."""
    plan = compile_pipeline(Pipeline.decode(safe_load(PIPELINE.replace(
        "resources: { cpu: 1, memory: 2GiB }", "resources: { cpu: 1, memory: 2GiB }, limit: api"
    ))))
    with pytest.raises(PlacementError, match="limit 'api'.*does not define") as e:
        place(plan, _deployment())
    assert "['tidy']" in str(e.value)
    defined = Deployment.decode({**_deployment().encode(), "limits": {"api": 2}})
    assert place(plan, defined).backend_of("tidy") == "local"
