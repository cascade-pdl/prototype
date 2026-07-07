"""Edge validation: do the type expressions on each edge agree?

Because both ends of an edge are declared against the same vocabulary in the same
pipeline, the check is type-expression name + arity equality — no structure
resolution, no subtyping, no registry. (Subtype-compatible edges are a future
choice; today a port's declared type must match exactly.)

Optionally, given a TypeEnv, each declared port's base type is checked for
membership (a declared structure or a known primitive) to catch typos — a set
lookup, not subtyping.

Reuses the edge-resolution helpers from elaborate so the arity logic lives in one
place. Takes prebuilt node_graphs (no rebuild).
"""
from __future__ import annotations

from cascade.graph import Graph
from cascade.model.pipeline import Pipeline
from cascade.model.dag_node import DagNode
from cascade.model.dependency import Dependency
from cascade.plan.signature import Signature, TypeExpr
from cascade.plan.type_env import TypeEnv
from cascade.plan.elaborate import _NodeInfo, _node_fans_out, resolve_edge


def validate_edges(
    pipeline: Pipeline,
    node_graphs: dict[str, Graph[DagNode, Dependency]],
    signatures: dict[str, Signature],
    type_env: TypeEnv | None = None,
) -> list[str]:
    errors: list[str] = []

    if type_env is not None:
        errors += _check_vocabulary(pipeline, type_env)

    for dag in pipeline.dags:
        graph = node_graphs[dag.name]
        dag_inputs = {p.name: TypeExpr.parse(p.type) for p in dag.input}

        info: dict[str, _NodeInfo] = {}
        for node_name in graph.static_order():
            node = graph.node(node_name)
            info[node.name] = _NodeInfo(
                sig=signatures[node.runnable_name],
                fan=_node_fans_out(node, info),
            )

        for node in dag.nodes:
            node_sig = signatures[node.runnable_name]
            for dep in node.depends_on:
                supplied, _fan = resolve_edge(dep, info, dag_inputs)
                port = dep.as_ or dep.field
                if port is None:
                    if len(node_sig.inputs) == 1:
                        (port,) = node_sig.inputs
                    else:
                        errors.append(
                            f"{dag.name}.{node.name}: edge from {dep.node!r} has no "
                            f"'as'/'field' to bind to one of {len(node_sig.inputs)} inputs"
                        )
                        continue
                if port not in node_sig.inputs:
                    errors.append(
                        f"{dag.name}.{node.name}: no input port {port!r} "
                        f"(runs {node.runnable_name!r})"
                    )
                    continue
                # a scattered port consumes one element of the supplied collection
                if node.scatter == port and supplied.depth >= 1:
                    supplied = supplied.element()
                expected = node_sig.inputs[port]
                if supplied != expected:
                    errors.append(
                        f"{dag.name}.{node.name}: port {port!r} expects "
                        f"{expected.render()}, got {supplied.render()} from {dep.node!r}"
                    )
    return errors


def _check_vocabulary(pipeline: Pipeline, type_env: TypeEnv) -> list[str]:
    errors: list[str] = []
    for ref in pipeline.refs:
        for port in (*ref.input, *ref.output):
            base = TypeExpr.parse(port.type).base
            if not type_env.is_defined(base):
                errors.append(f"ref {ref.name!r} port {port.name!r}: unknown type {base!r}")
    for dag in pipeline.dags:
        for port in dag.input:
            base = TypeExpr.parse(port.type).base
            if not type_env.is_defined(base):
                errors.append(f"dag {dag.name!r} input {port.name!r}: unknown type {base!r}")
    return errors
