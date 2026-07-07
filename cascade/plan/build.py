"""Build the graphs from the model, and the graph operations over them.

Two graphs, one source. The *node graphs* (one per dag) are built directly from
the pipeline and are the real structure — waves and signature derivation read
them. The *call graph* is always *derived* from the node graphs (`call_graph_of`),
never stored: it is a projection used for resolution order, cycle detection, and
reachability.

`build_call_and_node_graphs` is the single structural-validation pass: it builds
the node graphs once, checks every called runnable is declared (a set operation
against the pipeline's declared ref∪dag names — the one fact only authoring has),
checks acyclicity, and returns the resolution order plus the node graphs.
"""

from __future__ import annotations

from cascade.graph import Graph, GraphError
from cascade.model.pipeline import Pipeline
from cascade.model.dag import Dag
from cascade.model.dag_node import DagNode
from cascade.model.dependency import Dependency


def node_graph(dag: Dag) -> Graph[DagNode, Dependency]:
    """Per-dag structural graph: nodes are DagNodes, edges are intra-dag
    dependencies (upstream -> node). ``$input`` and out-of-dag references are
    boundary edges, resolved against the dag input by the elaborator, not graph
    edges. This is the same graph the executor runs ``waves()`` over."""
    g: Graph[DagNode, Dependency] = Graph()
    local = {n.name for n in dag.nodes}
    for n in dag.nodes:
        g.add_node(n.name, n)
    for n in dag.nodes:
        for dep in n.depends_on:
            if dep.is_input:
                continue
            if dep.node not in local:
                raise GraphError(
                    f"dag {dag.name!r}: node {n.name!r} depends on {dep.node!r}, "
                    f"which is not a node in this dag"
                )
            g.add_edge(dep.node, n.name, dep)  # upstream precedes node
    return g


def call_graph_of(
    node_graphs: dict[str, Graph[DagNode, Dependency]],
) -> Graph[str, None]:
    """Reconstruct the call graph from node graphs alone — no Pipeline.

    Edge is callee -> caller (so topological order is callees-first). Nodes are
    every dag (the keys) plus every runnable any node runs; a ref appears only if
    something calls it, which is all reachability and ordering need. This is the
    *trusting* projection: it cannot detect an undefined callee (one just looks
    like a leaf ref), because validation already happened at authoring time. Used
    at compile time (rooted at the entrypoint) and at runtime (rooted at a subdag
    for slicing) — one projection, many roots."""
    g: Graph[str, None] = Graph()
    for dag_name in node_graphs:
        if dag_name not in g:
            g.add_node(dag_name, None)
    for dag_name, ng in node_graphs.items():
        seen: set[str] = set()
        for _id, node in ng.nodes():
            callee = node.runnable_name
            if callee not in g:
                g.add_node(callee, None)  # a ref (leaf) or another dag
            if callee != dag_name and callee not in seen:
                seen.add(callee)
                g.add_edge(callee, dag_name, None)
    return g


def reachable_from(call: Graph[str, None], root: str) -> set[str]:
    """Every runnable reachable from ``root`` by following *calls* (root included).

    Call-graph edges are callee -> caller, so a runnable's callees are its
    *predecessors*. Reachability therefore walks the predecessor relation, not
    successors. The same op scopes the compile-time plan (root = entrypoint) and a
    runtime subdag slice (root = subdag name)."""
    preds = call.predecessors()  # {node: {predecessors}} == {caller: {its callees}}
    seen: set[str] = set()
    stack = [root]
    while stack:
        name = stack.pop()
        if name in seen:
            continue
        seen.add(name)
        stack.extend(preds.get(name, ()))
    return seen


def build_call_and_node_graphs(
    pipeline: Pipeline,
) -> tuple[list[str], dict[str, Graph[DagNode, Dependency]]]:
    """The single structural pass. Builds the node graphs once, then validates:
      * every called runnable is declared (set containment against ref∪dag names);
      * each node graph is acyclic (no intra-dag cycle);
      * the call graph is acyclic (no dag-call recursion).
    Returns the call-graph topological order (callees first) and the node graphs.
    No pass rebuilds these."""
    node_graphs = {dag.name: node_graph(dag) for dag in pipeline.dags}

    # declared-set check: the one fact only authoring has (refs that nothing calls
    # are invisible to call_graph_of, so an undefined callee must be caught here).
    declared = {r.name for r in pipeline.refs} | {d.name for d in pipeline.dags}
    for dag in pipeline.dags:
        for node in dag.nodes:
            if node.runnable_name not in declared:
                raise GraphError(
                    f"dag {dag.name!r}: node {node.name!r} runs "
                    f"{node.runnable_name!r}, which is not a defined ref or dag"
                )

    for name, g in node_graphs.items():
        g.check_acyclic()  # raises GraphCycleError on an intra-dag cycle

    order = call_graph_of(node_graphs).static_order()  # raises on a call cycle
    return order, node_graphs
