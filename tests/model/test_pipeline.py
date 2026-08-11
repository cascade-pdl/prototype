"""The pipeline lifecycle: decode a pipeline, compile it to a Plan, and assert on
what that Plan carries — chiefly ``dag_outputs``.

One fixture drives both halves. It is the moth-shaped pipeline (flat-bug detect ->
scatter/gather scoring) so the compile-side assertions exercise real structure:
two dags, a *gather* output (``analyse``) and a *single* output (``main``), a
subdag invoked as a node, and structural inheritance in the type section.

``dag_outputs`` carries how each dag's outputs wire onto its nodes — the executor
needs it and cannot recover it from ``node_graphs`` alone. It has silently
vanished twice on whole-folder syncs, so these tests double as its tripwire: the
next disappearance is a red bar, not a gap found later at execution time.
"""
from dataclasses import replace

import pytest
from yaml import safe_load

from cascade.model.pipeline import Pipeline
from cascade.model.dependency import Dependency
from cascade.plan.compile import compile_pipeline, check
from cascade.plan.plan import Plan, PLAN_VERSION, PlanVersionError
from cascade.plan.slice import slice_plan
from cascade.plan.integrity import check_plan_integrity


PIPELINE = """
entrypoint: main

types:
  structures:
    - name: BBox
      fields:
        - { name: x, type: "float" }
        - { name: y, type: "float" }
        - { name: w, type: "float" }
        - { name: h, type: "float" }
    - name: Detection
      fields:
        - { name: bbox,       type: "BBox" }
        - { name: label,      type: "string" }
        - { name: confidence, type: "float" }
    - name: Score
      extends: Detection                              # structural single-inheritance
      fields:
        - { name: species, type: "string" }
        - { name: score,   type: "float" }

input:
  - { name: image, type: "io.Image" }

refs:
  - name: detect
    runner: docker
    config: { image: "123.dkr.ecr.eu-west-1.amazonaws.com/flat-bug:v3" }
    input:  [ { name: image, type: "io.Image" } ]
    output: [ { name: dets,  type: "Detection[]" } ]
  - name: score
    runner: subprocess
    config: { cmd: ["python", "-m", "score"] }
    input:  [ { name: d, type: "Detection" } ]
    output:
      - { name: s, type: "Score", config: { encoding: "csv" } }

dags:
  - name: analyse
    input: [ { name: dets, type: "Detection[]" } ]
    nodes:
      - name: each
        runs: score
        scatter: d
        depends_on: [ { node: "$input", field: dets, as: d } ]
    output: [ { node: each, field: s, as: scores, mode: gather } ]
  - name: main
    input: [ { name: image, type: "io.Image" } ]
    nodes:
      - name: d
        runs: detect
        depends_on: [ { node: "$input", field: image, as: image } ]
      - name: a
        runs: analyse
        depends_on: [ { node: d, field: dets, as: dets } ]
    output: [ { node: a, field: scores, as: scores } ]
"""


@pytest.fixture
def pipe() -> Pipeline:
    return Pipeline.decode(safe_load(PIPELINE))


@pytest.fixture
def plan(pipe: Pipeline) -> Plan:
    return compile_pipeline(pipe)


# --- decode -----------------------------------------------------------------

def test_decode_reads_refs_dags_and_types(pipe: Pipeline):
    assert {r.name for r in pipe.refs} == {"detect", "score"}
    assert {d.name for d in pipe.dags} == {"analyse", "main"}
    assert pipe.types


def test_compile_accepts_the_pipeline(pipe: Pipeline):
    assert check(pipe) == []


# --- dag_outputs ------------------------------------------------------------

def test_dag_outputs_populated_for_every_dag_with_outputs(plan: Plan):
    assert set(plan.dag_outputs) == {"analyse", "main"}
    (gather,) = plan.dag_outputs["analyse"]
    (single,) = plan.dag_outputs["main"]
    assert (gather.node, gather.field, gather.mode) == ("each", "s", "gather")
    assert (single.node, single.field, single.mode) == ("a", "scores", "single")


def test_plan_is_version(plan: Plan):
    assert plan.version == PLAN_VERSION


def test_dag_outputs_round_trips(plan: Plan):
    wire = plan.encode()
    assert "dag_outputs" in wire, "dag_outputs dropped from encode()"
    assert wire["version"] == 2
    assert Plan.decode(wire) == plan


def test_slice_keeps_dag_outputs(plan: Plan):
    sliced = slice_plan(plan, "main")
    assert "main" in sliced.dag_outputs


def test_integrity_passes_on_clean_plan(plan: Plan):
    assert check_plan_integrity(plan) == []


def test_integrity_catches_dangling_output_edge(plan: Plan):
    bad = replace(plan, dag_outputs={"main": [Dependency(node="nonesuch", field="x")]})
    assert any("nonesuch" in e for e in check_plan_integrity(bad))


def test_integrity_catches_unknown_dag_key(plan: Plan):
    bad = replace(plan, dag_outputs={"ghostdag": []})
    assert any("ghostdag" in e for e in check_plan_integrity(bad))


def test_stale_v1_plan_is_rejected(plan: Plan):
    stale = plan.encode()
    stale["version"] = 1
    with pytest.raises(PlanVersionError):
        Plan.decode(stale)