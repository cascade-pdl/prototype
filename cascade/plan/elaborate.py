"""Signature derivation: prebuilt graphs + pipeline -> a signature map.

Functions over data, not a stateful object: the derived signatures are what cross
the boundary, the deriving behaviour stays here. Takes the graphs already built by
`build` (no rebuild). Derivation does *shape and arity* only — it can still fail on
missing output fields or scatter over a non-collection. Type *identity* (is-a) is a
separate pass (plan.validate).

**Fan is one node deep.** It opens where a node declares ``scatter`` and closes at
that same node's boundary, where the fan runner gathers the lanes. So a node fans iff
``node.scatter is not None`` — a local property, read directly rather than propagated
along edges — and every edge obeys one rule: a fanned producer delivers one more array
level than it declares, an ordinary producer delivers what it declares. There is no
open fan to track, so there is no fan status to compute, and an unclosed fan at a dag
boundary is unrepresentable rather than merely rejected.
"""
from __future__ import annotations

from dataclasses import dataclass

from cascade.graph import Graph
from cascade.model.pipeline import Pipeline
from cascade.model.refs import Ref
from cascade.model.dag import Dag
from cascade.model.dag_node import DagNode
from cascade.model.dependency import Dependency
from cascade.plan.signature import Signature, TypeExpr


class ElaborationError(Exception):
    """A signature could not be derived (shape/arity)."""


MERGE_NEST = "nest"
MERGE_FLATTEN = "flatten"
MERGES = (MERGE_NEST, MERGE_FLATTEN)


@dataclass
class _NodeInfo:
    sig: Signature
    fan: bool  # node.scatter is not None -- read, never propagated
    merge: str = MERGE_NEST  # how this node's gather shapes its output


def elaborate(
    pipeline: Pipeline,
    node_graphs: dict[str, Graph[DagNode, Dependency]],
    order: list[str],
) -> dict[str, Signature]:
    """Derive a Signature for every runnable. ``order`` is the call-graph
    topological order (callees first); ``node_graphs`` are the prebuilt per-dag
    graphs. Neither is rebuilt here."""
    refs = {r.name: r for r in pipeline.refs}
    dags = {d.name: d for d in pipeline.dags}
    sigs: dict[str, Signature] = {}
    for name in order:
        if name in refs:
            sigs[name] = _from_ref(refs[name])
        else:
            sigs[name] = _from_dag(dags[name], node_graphs[name], sigs)
    return sigs


def _from_ref(ref: Ref) -> Signature:
    return Signature(
        inputs={p.name: p.type for p in ref.input},
        outputs={p.name: p.type for p in ref.output},
    )


def _from_dag(dag: Dag, graph: Graph[DagNode, Dependency], sigs: dict[str, Signature]) -> Signature:
    dag_inputs = {p.name: p.type for p in dag.input}

    info: dict[str, _NodeInfo] = {}
    for node_name in graph.static_order():  # raises GraphCycleError on a node cycle
        node = graph.node(node_name)
        info[node.name] = _NodeInfo(
            sig=sigs[node.runnable_name],  # already resolved (call-graph order)
            fan=node.scatter is not None,
            merge=node.merge,
        )
        _check_merge(node, info[node.name].sig)
        _check_scatter(node, info, dag_inputs)

    outputs: dict[str, TypeExpr] = {}
    for dep in dag.output:
        outputs[dep.as_ or dep.field or dep.node] = resolve_edge(dep, info, dag_inputs)

    return Signature(inputs=dag_inputs, outputs=outputs)


def resolve_edge(
    dep: Dependency, info: dict[str, _NodeInfo], dag_inputs: dict[str, TypeExpr]
) -> TypeExpr:
    """The type flowing along one edge.

    A fanned producer's output is gathered at its own boundary, so how it arrives depends
    on that node's merge policy: ``nest`` wraps the N lane values, arriving one array
    level deeper than declared; ``flatten`` concatenates them, arriving as declared. An
    unfanned producer arrives as declared.
    """
    if dep.is_input:
        if dep.field not in dag_inputs:
            raise ElaborationError(f"$input has no field {dep.field!r}")
        return dag_inputs[dep.field]

    up = info.get(dep.node)
    if up is None:
        raise ElaborationError(f"dependency on unknown or forward node {dep.node!r}")
    field = dep.field
    if field is None:
        if len(up.sig.outputs) != 1:
            raise ElaborationError(
                f"edge from {dep.node!r} omits 'field' but it has {len(up.sig.outputs)} outputs"
            )
        (field,) = up.sig.outputs
    if field not in up.sig.outputs:
        raise ElaborationError(f"node {dep.node!r} has no output {field!r}")
    t = up.sig.outputs[field]
    if not up.fan:
        return t
    return t if up.merge == MERGE_FLATTEN else t.as_collection()


def _check_merge(node: DagNode, sig: Signature) -> None:
    if node.merge not in MERGES:
        raise ElaborationError(
            f"{node.name}: unknown merge {node.merge!r}; expected one of {MERGES}"
        )
    if node.merge != MERGE_FLATTEN:
        return
    if node.scatter is None:
        raise ElaborationError(
            f"{node.name}: merge {MERGE_FLATTEN!r} is meaningless without 'scatter'"
        )
    # flattening concatenates lane values, so each must itself be a collection;
    # flattening scalars would just be nesting under a misleading name
    for port, t in sig.outputs.items():
        if t.depth < 1:
            raise ElaborationError(
                f"{node.name}: merge {MERGE_FLATTEN!r} needs collection outputs, but "
                f"port {port!r} is {t.render()}"
            )


def _check_scatter(node: DagNode, info: dict[str, _NodeInfo], dag_inputs: dict[str, TypeExpr]) -> None:
    if node.scatter is None:
        return
    for dep in node.depends_on:
        if (dep.as_ or dep.field) == node.scatter:
            t = resolve_edge(dep, info, dag_inputs)
            if t.depth < 1:
                raise ElaborationError(
                    f"node {node.name!r} scatters over {node.scatter!r}, "
                    f"but it resolves to non-collection {t.render()!r}"
                )
            return