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
from pathlib import Path

from cascade.graph import GraphError
from cascade.model.dag import Dag
from cascade.model.dag_node import DagNode
from cascade.model.dependency import Dependency
from cascade.model.pipeline import Pipeline
from cascade.model.ref_data import RefDocker
from cascade.plan.build import build_call_and_node_graphs
from cascade.plan.build_context import check_context, context_hash
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


def _built_refs(pipeline: Pipeline) -> list[tuple[str, RefDocker]]:
    return [
        (r.name, r.config)
        for r in pipeline.refs
        if isinstance(r.config, RefDocker) and r.config.build is not None
    ]


def _check_builds(pipeline: Pipeline, base: Path | None) -> list[str]:
    """``build:`` directories, resolved against ``base`` (the pipeline's directory)."""
    errors = []
    for name, ref in _built_refs(pipeline):
        if base is None:
            errors.append(
                f"ref {name!r} builds its image from {ref.build!r}, which is relative to "
                "the pipeline's directory, and none was given"
            )
        else:
            errors += [f"ref {name!r}: {e}" for e in check_context(base / ref.build)]
    return errors


def _run_config(pipeline: Pipeline, base: Path | None) -> dict[str, RunConfig]:
    """Each ref's run config, with every ``build:`` ref's content hash recorded: the
    image's name is derived from it, so the Plan says exactly which image runs."""
    run_config = {r.name: RunConfig.from_ref(r) for r in pipeline.refs}
    for name, ref in _built_refs(pipeline):
        built = replace(ref, build_hash=context_hash(base / ref.build))
        run_config[name] = replace(run_config[name], config=built)
    return run_config


def compile_pipeline(pipeline: Pipeline, base: Path | None = None) -> Plan:
    """``base`` is the pipeline's directory: ``build:`` paths resolve against it."""
    errors = check(pipeline, base)
    if errors:
        raise CompileError(errors)
    pipeline, _ = _ref_entrypoint_as_dag(pipeline)
    # safe to build the artifacts now; check() already proved they derive
    order, node_graphs = build_call_and_node_graphs(pipeline)
    signatures = elaborate(pipeline, node_graphs, order)
    type_env = resolve_types(pipeline)
    run_config = _run_config(pipeline, base)
    dag_outputs = {d.name: d.output for d in pipeline.dags}
    return Plan(
        entrypoint=pipeline.entrypoint,
        node_graphs=node_graphs,
        signatures=signatures,
        run_config=run_config,
        dag_outputs=dag_outputs,
        type_env=type_env,
        limits=dict(pipeline.limits),
    )


def check(pipeline: Pipeline, base: Path | None = None) -> list[str]:
    """Validate without raising; returns a list of error messages (empty = OK).

    ``base`` is the pipeline's directory, needed only by refs that ``build:`` their
    image."""
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
    errors = validate_edges(pipeline, node_graphs, signatures, type_env)  # name + arity
    return errors + _check_builds(pipeline, base)
