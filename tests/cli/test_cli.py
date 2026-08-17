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
from cascade.model.pipeline import Pipeline
from cascade.model.dependency import Dependency
from cascade.plan.compile import check
from cascade.plan.plan import Plan, PLAN_VERSION, PlanVersionError
from cascade.plan.slice import slice_plan
from cascade.plan.integrity import check_plan_integrity


# --- decode -----------------------------------------------------------------

def test_decode_reads_refs_dags_and_types(pipeline: Pipeline):
    assert {r.name for r in pipeline.refs} == {"detect", "score"}
    assert {d.name for d in pipeline.dags} == {"analyse", "main"}
    assert pipeline.types


def test_compile_accepts_the_pipeline(pipeline: Pipeline):
    assert check(pipeline=pipeline) == []


# --- dag_outputs ------------------------------------------------------------

def test_dag_outputs_populated_for_every_dag_with_outputs(plan: Plan):
    assert set(plan.dag_outputs) == {"analyse", "main"}
    (from_fan,) = plan.dag_outputs["analyse"]
    (plain,) = plan.dag_outputs["main"]
    # no mode: 'analyse' exports a scattered node, so the gather is implied by
    # 'each' declaring scatter -- the edge itself has no choice to make
    assert (from_fan.node, from_fan.field) == ("each", "s")
    assert (plain.node, plain.field) == ("a", "scores")


def test_plan_is_version(plan: Plan):
    assert plan.version == PLAN_VERSION


def test_dag_outputs_round_trips(plan: Plan):
    wire = plan.encode()
    assert "dag_outputs" in wire, "dag_outputs dropped from encode()"
    assert wire["version"] == 5
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