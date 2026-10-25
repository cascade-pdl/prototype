"""Orchestration: Pipeline -> Plan.

Builds every graph exactly once and threads it through the passes:
    build (node graphs + structural validation: declared-set, acyclicity)
    -> elaborate (signatures; shape + arity)
    -> validate_edges (type-expression name + arity)
    -> resolve_types (the runtime type environment)
    -> run config (per-ref launch config)
    -> Plan

``compile_pipeline`` raises on any problem; ``check`` returns the problems as a
list (for a `cascade validate` style command). The Plan produced is the *full*
plan (every declared runnable); scoping to the reachable set is a separate slice
(see plan.slice) so the same op serves compile-time trimming and runtime subdag
slicing.
"""
from __future__ import annotations

from dataclasses import replace

from cascade.graph import GraphError
from cascade.model.dag import Dag
from cascade.model.dag_node import DagNode
from cascade.model.dependency import Dependency
from cascade.model.pipeline import Pipeline
from cascade.plan.build import build_call_and_node_graphs
from cascade.plan.elaborate import elaborate, ElaborationError
from cascade.plan.validate import validate_edges
from cascade.plan.type_env import resolve_types
from cascade.plan.run_config import RunConfig
from cascade.plan.plan import Plan


class CompileError(Exception):
    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__("; ".join(errors))


ENTRY_DAG = "main"
"""The implicit dag a ref entrypoint runs in. The executor runs dags only, and the store
lays a run out under its entrypoint dag (``<run>/main/<node>/...``), so a single ref is
given the same shape as any pipeline rather than a special case of its own."""


def _ref_entrypoint_as_dag(pipeline: Pipeline) -> tuple[Pipeline, list[str]]:
    """A pipeline whose entrypoint names a ref, rewritten to run it in a one-node dag.

    The node is named after the ref; the ref's inputs become the dag's inputs and its
    outputs the dag's outputs, so the run takes and returns exactly what the ref does.
    Anything else is returned unchanged.
    """
    ref = pipeline.find_ref(pipeline.entrypoint)
    if ref is None:
        return pipeline, []
    if pipeline.find(ENTRY_DAG) is not None:
        return pipeline, [
            f"entrypoint {ref.name!r} is a ref, which runs in an implicit dag named "
            f"{ENTRY_DAG!r}, but a runnable is already called {ENTRY_DAG!r}; rename it"
        ]
    dag = Dag(
        name=ENTRY_DAG,
        nodes=[DagNode(
            name=ref.name,
            depends_on=[Dependency(node="$input", field=p.name, as_=p.name) for p in ref.input],
        )],
        input=list(ref.input),
        output=[Dependency(node=ref.name, field=p.name, as_=p.name) for p in ref.output],
    )
    return replace(pipeline, entrypoint=ENTRY_DAG, dags=[*pipeline.dags, dag]), []


def compile_pipeline(pipeline: Pipeline) -> Plan:
    errors = check(pipeline)
    if errors:
        raise CompileError(errors)
    pipeline, _ = _ref_entrypoint_as_dag(pipeline)
    # safe to build the artifacts now; check() already proved they derive
    order, node_graphs = build_call_and_node_graphs(pipeline)
    signatures = elaborate(pipeline, node_graphs, order)
    type_env = resolve_types(pipeline)
    run_config = {r.name: RunConfig.from_ref(r) for r in pipeline.refs}
    dag_outputs = {d.name: d.output for d in pipeline.dags}
    return Plan(
        entrypoint=pipeline.entrypoint,
        node_graphs=node_graphs,
        signatures=signatures,
        run_config=run_config,
        dag_outputs=dag_outputs,
        type_env=type_env,
    )


def check(pipeline: Pipeline) -> list[str]:
    """Validate without raising; returns a list of error messages (empty = OK)."""
    if pipeline.find(pipeline.entrypoint) is None:
        return [f"entrypoint {pipeline.entrypoint!r} names no ref or dag"]
    pipeline, errors = _ref_entrypoint_as_dag(pipeline)
    if errors:
        return errors
    try:
        order, node_graphs = build_call_and_node_graphs(pipeline)  # structural
        signatures = elaborate(pipeline, node_graphs, order)        # shape + arity
    except (GraphError, ElaborationError) as exc:
        return [str(exc)]
    type_env = resolve_types(pipeline)
    return validate_edges(pipeline, node_graphs, signatures, type_env)  # name + arity
