"""Signature derivation: prebuilt graphs + pipeline -> a signature map.

Functions over data, not a stateful object: the derived signatures are what cross
the boundary, the deriving behaviour stays here. Takes the graphs already built by
`build` (no rebuild). Derivation does *shape and arity* only — it can still fail on
missing output fields, an ungathered fan at a dag boundary, or scatter over a
non-collection. Type *identity* (is-a) is a separate pass (plan.validate).
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


@dataclass
class _NodeInfo:
    sig: Signature
    fan: bool


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
        inputs={p.name: TypeExpr.parse(p.type) for p in ref.input},
        outputs={p.name: TypeExpr.parse(p.type) for p in ref.output},
    )


def _from_dag(dag: Dag, graph: Graph[DagNode, Dependency], sigs: dict[str, Signature]) -> Signature:
    dag_inputs = {p.name: TypeExpr.parse(p.type) for p in dag.input}

    info: dict[str, _NodeInfo] = {}
    for node_name in graph.static_order():  # raises GraphCycleError on a node cycle
        node = graph.node(node_name)
        info[node.name] = _NodeInfo(
            sig=sigs[node.runnable_name],  # already resolved (call-graph order)
            fan=_node_fans_out(node, info),
        )
        _check_scatter(node, info, dag_inputs)

    outputs: dict[str, TypeExpr] = {}
    for dep in dag.output:
        t, unclosed_fan = resolve_edge(dep, info, dag_inputs)
        if unclosed_fan:
            raise ElaborationError(
                f"dag {dag.name!r} exports {dep.node}.{dep.field} from a fanned-out "
                f"node without 'gather' — fan dimension undefined at the boundary"
            )
        outputs[dep.as_ or dep.field or dep.node] = t

    return Signature(inputs=dag_inputs, outputs=outputs)


def resolve_edge(
    dep: Dependency, info: dict[str, _NodeInfo], dag_inputs: dict[str, TypeExpr]
) -> tuple[TypeExpr, bool]:
    """Type and fan-status flowing along one edge. gather -> +1 array level, fan
    closed; single -> element pass-through, inherits any open upstream fan."""
    if dep.is_input:
        if dep.field not in dag_inputs:
            raise ElaborationError(f"$input has no field {dep.field!r}")
        return dag_inputs[dep.field], False

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

    if dep.mode == "gather":
        return t.as_collection(), False
    if dep.mode == "single":
        return t, up.fan
    raise ElaborationError(f"unknown dependency mode {dep.mode!r}")


def _node_fans_out(node: DagNode, info: dict[str, _NodeInfo]) -> bool:
    if node.scatter is not None:
        return True
    for dep in node.depends_on:
        if dep.mode == "single" and not dep.is_input:
            up = info.get(dep.node)
            if up is not None and up.fan:
                return True
    return False


def _check_scatter(node: DagNode, info: dict[str, _NodeInfo], dag_inputs: dict[str, TypeExpr]) -> None:
    if node.scatter is None:
        return
    for dep in node.depends_on:
        if (dep.as_ or dep.field) == node.scatter:
            t, _ = resolve_edge(dep, info, dag_inputs)
            if t.depth < 1:
                raise ElaborationError(
                    f"node {node.name!r} scatters over {node.scatter!r}, "
                    f"but it resolves to non-collection {t.render()!r}"
                )
            return
