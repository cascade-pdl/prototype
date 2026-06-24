"""Build the shared ``Graph`` from the model.

These builders are also the *structural validation* pass: constructing the
call graph surfaces unknown refs/dags, and the topological order (via the
graph's graphlib backing) surfaces cycles — before any type work happens.

There is one graph *type* (cascade.plan.graph.Graph) and these are the only
places a graph is built from the model, so traversal/ordering/cycle logic lives
in exactly one implementation.
"""
from __future__ import annotations

from cascade.model.pipeline import Pipeline
from cascade.model.dag import Dag
from cascade.model.dag_node import DagNode
from cascade.model.dependency import Dependency
from cascade.plan.graph import Graph, GraphError


def node_graph(dag: Dag) -> Graph[DagNode, Dependency]:
    """Per-dag structural graph: nodes are DagNodes, edges are *intra-dag*
    dependencies (upstream -> node). ``$input`` and references to nodes outside
    this dag are boundary edges, not graph edges — they resolve against the dag
    input, handled by the elaborator. This is the same graph the nested executor
    runs ``waves()`` over.
    """
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


def call_graph(pipeline: Pipeline) -> Graph[str, None]:
    """Dag-call graph across runnables. Edge is callee -> caller, so topological
    order yields callees (refs, inner dags) before callers — the order in which
    signatures must be resolved. Building it validates that every node's
    ``ref_name`` resolves to a real runnable.
    """
    g: Graph[str, None] = Graph()
    for r in (*pipeline.refs, *pipeline.dags):
        g.add_node(r.name, None)
    for dag in pipeline.dags:
        seen: set[str] = set()
        for node in dag.nodes:
            callee = node.ref_name
            if callee not in g:
                raise GraphError(
                    f"dag {dag.name!r}: node {node.name!r} runs {callee!r}, "
                    f"which is not a defined ref or dag"
                )
            if callee != dag.name and callee not in seen:
                seen.add(callee)
                g.add_edge(callee, dag.name, None)  # callee precedes caller
    return g
