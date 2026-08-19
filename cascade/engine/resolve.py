"""Input resolution — where each of a node's ports reads from.

This is the dag runner's input logic, kept as free functions rather than methods so
it can be tested without a store, an event loop or a spawn. That is its only reason
to be a separate module.

**There is no fan decision here.** A fan is one node deep: it opens at a node's
``scatter`` and closes at that same node's boundary, where the fan runner gathers the
lanes and writes one collection at the node's own scope. So a consumer binds to a
fanned producer exactly as it binds to any other — same scope, same key — and whether
a node fans is a local read of ``node.scatter``, made by the dag runner. Earlier
drafts tracked fan status through resolution because fan could propagate along edges;
it no longer can, and roughly two thirds of this module went with it.

Consequently a binding is *static*: a node's output always lives at its own name
relative to the dag's slot, so resolution needs the plan and the graph but no runtime
results at all.

Each binding carries the consuming port's declared ``encoding`` and ``depth``, taken
straight from the signature: the store holds canonical JSON, so ``encoding`` tells the node
what its *tool* wants on local disk, and ``depth`` is what lets it check that what arrived
matches what was declared.

**Dag outputs are aliases.** A dag's output port names an inner node's output, so the
bytes are already in place and the location is derivable from the plan rather than
recorded or copied. ``resolve_dag_output`` chases that chain (a dag exporting a dag
exporting a node).
"""
from __future__ import annotations

from cascade.graph import Graph
from cascade.model.dag_node import DagNode
from cascade.model.dependency import Dependency
from cascade.plan.plan import Plan

from cascade.engine.binding import InputBinding, InputBindings


class ResolveError(Exception):
    """A node's inputs cannot be resolved against the plan."""


def resolve_dag_output(plan: Plan, dag: str, port: str) -> tuple[tuple[str, ...], str]:
    """Where a dag's output port physically lives, relative to that dag's own slot.

    Chases aliases: if the exporting node is itself a dag, recurse into it. Returns
    ``(scope, key)`` — nothing is written or copied.
    """
    for dep in plan.dag_outputs.get(dag, []):
        if (dep.as_ or dep.field) != port:
            continue
        if dep.is_input:
            raise ResolveError(
                f"dag {dag!r} exports port {port!r} straight from its own input; "
                "pass-through exports are not resolvable to a node scope"
            )
        if dep.field is None:
            raise ResolveError(f"dag {dag!r} output {port!r} names no field")
        inner = plan.node_graphs[dag].node(dep.node)
        runnable = inner.runnable_name
        if runnable in plan.node_graphs and inner.scatter is None:
            # the exporting node is an un-fanned dag: descend into it. A *fanned* dag
            # node is opaque -- its gathered collection sits at the node's own scope.
            deeper, key = resolve_dag_output(plan, runnable, dep.field)
            return (dep.node, *deeper), key
        return (dep.node,), dep.field
    raise ResolveError(f"dag {dag!r} has no output port {port!r}")


def output_location(
    plan: Plan,
    graph: Graph[DagNode, Dependency],
    dep: Dependency,
) -> tuple[tuple[str, ...], str]:
    """Where an upstream node's output lives, relative to the dag's slot."""
    upstream = graph.node(dep.node)
    runnable = upstream.runnable_name
    signature = plan.signatures.get(runnable)
    if signature is None:
        raise ResolveError(f"no signature for runnable {runnable!r}")
    if dep.field is None:
        raise ResolveError(f"dependency on {dep.node!r} names no field")
    if dep.field not in signature.outputs:
        raise ResolveError(f"{runnable!r} has no output port {dep.field!r}")

    if runnable in plan.node_graphs and upstream.scatter is None:
        inner, key = resolve_dag_output(plan, runnable, dep.field)
        return (dep.node, *inner), key
    return (dep.node,), dep.field


def resolve_node(
    node: DagNode,
    plan: Plan,
    graph: Graph[DagNode, Dependency],
    dag_inputs: InputBindings | None = None,
) -> InputBindings:
    """The input bindings for one node: one per declared dependency."""
    signature = plan.signatures.get(node.runnable_name)
    if signature is None:
        raise ResolveError(f"no signature for runnable {node.runnable_name!r}")
    dag_inputs = dag_inputs or InputBindings()

    bindings = []
    for dep in node.depends_on:
        port = dep.as_ or dep.field
        if port is None:
            raise ResolveError(f"{node.name}: dependency on {dep.node!r} names no port")
        declared = signature.inputs.get(port)
        if declared is None:
            raise ResolveError(f"{node.name}: no input port {port!r}")

        if dep.is_input:
            bound = dag_inputs.input_for(dep.field or port)
            if bound is None:
                raise ResolveError(
                    f"{node.name}: port {port!r} reads dag input "
                    f"{dep.field or port!r}, which was not bound"
                )
            scope, key = bound.scope, bound.key
        else:
            scope, key = output_location(plan, graph, dep)

        bindings.append(
            InputBinding(
                port=port,
                scope=tuple(scope),
                key=key,
                type=declared.type,
                config=declared.config,
            )
        )
    return InputBindings(inputs=tuple(bindings))