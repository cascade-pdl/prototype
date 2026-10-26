"""Ref resources and the derived runner kind."""
import pytest
from yaml import safe_load

from cascade.model.pipeline import Pipeline
from cascade.model.ref_data import RefDocker, RefEcho, RefSubprocess
from cascade.model.refs import Ref
from cascade.model.resources import Resources, ResourcesError, parse_memory
from cascade.model.runner_kinds import RunnerKind
from cascade.plan.compile import check, compile_pipeline
from cascade.plan.plan import Plan
from cascade.plan.run_config import RunConfig


# --- memory quantities ---------------------------------------------------------

@pytest.mark.parametrize("raw, mib", [
    ("512MiB", 512), ("4GiB", 4096), ("1TiB", 1024 * 1024), ("1.5GiB", 1536),
    ("2048KiB", 2), ("1GB", 954), ("1MB", 1),   # decimal units round *up*
])
def test_memory_parses_to_mib(raw, mib):
    assert parse_memory(raw) == mib


@pytest.mark.parametrize("raw", [4096, "4096", "4 gigs", "4Gb", "0GiB"])
def test_memory_needs_a_positive_quantity_with_a_unit(raw):
    """A bare 4096 could be MB, MiB or bytes; refusing it is the only safe reading."""
    with pytest.raises(ResourcesError):
        parse_memory(raw)


def test_resources_round_trip():
    r = Resources.decode({"cpu": 0.5, "memory": "1536MiB", "gpu": 1})
    assert r == Resources(cpu=0.5, memory=1536, gpu=1)
    assert Resources.decode(r.encode()) == r
    assert Resources(cpu=2, memory=4096).encode() == {"cpu": 2, "memory": "4GiB", "gpu": 0}


@pytest.mark.parametrize("raw", [
    {"memory": "1GiB"}, {"cpu": 1}, {"cpu": 0, "memory": "1GiB"},
    {"cpu": 1, "memory": "1GiB", "gpu": 0.5}, {"cpu": 1, "memory": "1GiB", "disk": "1GiB"},
    {"cpu": True, "memory": "1GiB"},
])
def test_resources_reject_what_they_cannot_mean(raw):
    with pytest.raises(ResourcesError):
        Resources.decode(raw)


def test_shortfalls_name_each_resource_that_does_not_fit():
    cap = Resources(cpu=4, memory=16384)
    assert cap.shortfalls(Resources(cpu=4, memory=16384)) == []
    assert cap.shortfalls(Resources(cpu=8, memory=32768, gpu=1)) == [
        "cpu 8 > max 4", "memory 32GiB > max 16GiB", "gpu 1 > max 0",
    ]


# --- the runner kind is derived ----------------------------------------------

def test_the_kind_is_the_ref_datas_class():
    assert RunConfig(config=RefDocker(image="x")).runner is RunnerKind.docker
    assert RunConfig(config=RefSubprocess(cmd=["x"])).runner is RunnerKind.subprocess
    assert Ref(name="e", config=RefEcho()).runner is RunnerKind.echo


def test_the_wire_format_still_carries_the_tag():
    """Derived in memory, still written out: a Plan stays readable on its own."""
    encoded = RunConfig(config=RefDocker(image="x"), resources=Resources(1, 1024)).encode()
    assert encoded["runner"] == "docker"
    assert RunConfig.decode(encoded).runner is RunnerKind.docker


# --- compile-time rules ------------------------------------------------------

PIPELINE = """
entrypoint: main
input: []
types: { structures: [] }
limits: { heavy: 1 }
refs:
  - name: r
    runner: %RUNNER%
    config: %CONFIG%
    %EXTRA%
    input: []
    output: [ { name: o, type: string } ]
dags:
  - name: main
    input: []
    nodes: [ { name: n, runs: r } ]
    output: [ { node: n, field: o, as: out } ]
"""

CONFIGS = {"docker": '{ image: "x:1" }', "subprocess": '{ cmd: ["true"] }', "echo": "{}"}


def _pipeline(runner: str, extra: str = "") -> Pipeline:
    text = (PIPELINE.replace("%RUNNER%", runner).replace("%CONFIG%", CONFIGS[runner])
            .replace("%EXTRA%", extra))
    return Pipeline.decode(safe_load(text))


@pytest.mark.parametrize("runner", ["docker", "subprocess"])
def test_work_must_state_its_resources(runner):
    errors = check(_pipeline(runner))
    assert len(errors) == 1 and "must state `resources" in errors[0]


@pytest.mark.parametrize(
    "extra", ["resources: { cpu: 1, memory: 1GiB }", "requires: [www]", "limit: heavy"]
)
def test_a_stub_states_nothing(extra):
    errors = check(_pipeline("echo", extra))
    assert len(errors) == 1 and "never placed" in errors[0]


def test_resources_and_requires_travel_in_the_plan():
    plan = compile_pipeline(
        _pipeline("docker", "resources: { cpu: 2, memory: 8GiB, gpu: 1 }\n    requires: [www]")
    )
    back = Plan.decode(plan.encode())
    assert back.run_config["r"].resources == Resources(cpu=2, memory=8192, gpu=1)
    assert back.run_config["r"].requires == ("www",)


def test_a_limit_label_travels_in_the_plan():
    plan = compile_pipeline(
        _pipeline("docker", "resources: { cpu: 1, memory: 1GiB }\n    limit: heavy")
    )
    assert Plan.decode(plan.encode()).run_config["r"].limit == "heavy"


@pytest.mark.parametrize("label", ["", 3, ["heavy"]])
def test_a_limit_is_one_label(label):
    with pytest.raises(ValueError, match="`limit` must be a label"):
        Ref.decode({"name": "r", "runner": "echo", "config": {}, "limit": label})
