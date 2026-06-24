"""Build the shared Graph from the model. These builders are also the structural
validation pass: building the call graph surfaces unknown runnables, and the
topological order surfaces cycles — before any type work.
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


def build_call_and_node_graphs(
    pipeline: Pipeline,
) -> tuple[list[str], dict[str, Graph[DagNode, Dependency]]]:
    """Build every graph once: returns the call-graph topological order (callees
    first) and the per-dag node graphs. This is the single place graphs are built
    for a compile, so no pass rebuilds them."""
    order = call_graph(pipeline).static_order()
    node_graphs = {dag.name: node_graph(dag) for dag in pipeline.dags}
    return order, node_graphs


def call_graph(pipeline: Pipeline) -> Graph[str, None]:
    """Dag-call graph across runnables; edge callee -> caller, so topological
    order yields callees before callers. Building it validates that every node's
    runnable resolves to a defined ref or dag."""
    g: Graph[str, None] = Graph()
    for r in (*pipeline.refs, *pipeline.dags):
        g.add_node(r.name, None)
    for dag in pipeline.dags:
        seen: set[str] = set()
        for node in dag.nodes:
            callee = node.runnable_name
            if callee not in g:
                raise GraphError(
                    f"dag {dag.name!r}: node {node.name!r} runs {callee!r}, "
                    f"which is not a defined ref or dag"
                )
            if callee != dag.name and callee not in seen:
                seen.add(callee)
                g.add_edge(callee, dag.name, None)
    return g
