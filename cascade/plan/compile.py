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

from cascade.graph import GraphError
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


def compile_pipeline(pipeline: Pipeline) -> Plan:
    errors = check(pipeline)
    if errors:
        raise CompileError(errors)
    # safe to build the artifacts now; check() already proved they derive
    order, node_graphs = build_call_and_node_graphs(pipeline)
    signatures = elaborate(pipeline, node_graphs, order)
    type_env = resolve_types(pipeline)
    run_config = {r.name: RunConfig.from_ref(r) for r in pipeline.refs}
    return Plan(
        entrypoint=pipeline.entrypoint,
        node_graphs=node_graphs,
        signatures=signatures,
        run_config=run_config,
        type_env=type_env,
    )


def check(pipeline: Pipeline) -> list[str]:
    """Validate without raising; returns a list of error messages (empty = OK)."""
    if pipeline.find(pipeline.entrypoint) is None:
        return [f"entrypoint {pipeline.entrypoint!r} names no ref or dag"]
    try:
        order, node_graphs = build_call_and_node_graphs(pipeline)  # structural
        signatures = elaborate(pipeline, node_graphs, order)        # shape + arity
    except (GraphError, ElaborationError) as exc:
        return [str(exc)]
    type_env = resolve_types(pipeline)
    return validate_edges(pipeline, node_graphs, signatures, type_env)  # name + arity
