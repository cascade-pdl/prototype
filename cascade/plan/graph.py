"""A small directed-graph container for the pipeline planner.

The *structure* (nodes, edges, ordering, cycle detection) is generic and knows
nothing about types or refs. The *domain* lives entirely in the payloads it is
parameterised with — node payload ``N`` and edge payload ``E`` — so the same
class serves every consumer the pipeline needs:

    * the model graph        (pass 1: structural validation, derivation order)
    * the flattened exec graph(pass 4: subdags inlined, scatter expanded)

Ordering and cycle detection delegate to ``graphlib.TopologicalSorter`` (stdlib,
purpose-built for task DAGs). ``waves()`` is the frontier form the executor wants:
each yielded tuple is a set of mutually-independent nodes safe to dispatch together.

Feed the *flattened* graph to ``waves()`` — node ids there are concrete instances
(e.g. ``"analyse.each#0"``), so a wave is directly a batch of schedulable work.
"""
from __future__ import annotations

from dataclasses import dataclass
from graphlib import TopologicalSorter, CycleError
from typing import Generic, Iterator, TypeVar

N = TypeVar("N")  # node payload (e.g. a resolved node instance)
E = TypeVar("E")  # edge payload (e.g. resolved type + single/gather mode)


class GraphError(Exception):
    """Structural problem with the graph."""


class GraphCycleError(GraphError):
    def __init__(self, cycle: list[str]):
        self.cycle = cycle
        super().__init__("cycle through: " + " -> ".join(cycle))


@dataclass(frozen=True)
class Edge(Generic[E]):
    src: str
    dst: str
    payload: E


class Graph(Generic[N, E]):
    def __init__(self) -> None:
        self._nodes: dict[str, N] = {}
        self._out: dict[str, list[Edge[E]]] = {}
        self._in: dict[str, list[Edge[E]]] = {}

    # ---------------------------------------------------------------- building
    def add_node(self, node_id: str, payload: N) -> None:
        if node_id in self._nodes:
            raise GraphError(f"duplicate node {node_id!r}")
        self._nodes[node_id] = payload
        self._out[node_id] = []
        self._in[node_id] = []

    def add_edge(self, src: str, dst: str, payload: E) -> None:
        if src not in self._nodes:
            raise GraphError(f"edge from unknown node {src!r}")
        if dst not in self._nodes:
            raise GraphError(f"edge to unknown node {dst!r}")
        edge = Edge(src, dst, payload)
        self._out[src].append(edge)
        self._in[dst].append(edge)

    # ----------------------------------------------------------------- access
    def __contains__(self, node_id: str) -> bool:
        return node_id in self._nodes

    def __len__(self) -> int:
        return len(self._nodes)

    def node(self, node_id: str) -> N:
        return self._nodes[node_id]

    def nodes(self) -> Iterator[tuple[str, N]]:
        return iter(self._nodes.items())

    def in_edges(self, node_id: str) -> list[Edge[E]]:
        return self._in[node_id]

    def out_edges(self, node_id: str) -> list[Edge[E]]:
        return self._out[node_id]

    def successors(self, node_id: str) -> list[str]:
        return [e.dst for e in self._out[node_id]]

    # -------------------------------------------------- graphlib bridge
    def predecessors(self) -> dict[str, set[str]]:
        """The {node: {predecessors}} view that feeds TopologicalSorter directly.
        Parallel edges (two edges between the same pair) collapse — ordering only
        cares that a precedes b, not how many times."""
        return {n: {e.src for e in self._in[n]} for n in self._nodes}

    def check_acyclic(self) -> None:
        """Raise GraphCycleError if the graph has a cycle; cheap, consumes nothing."""
        try:
            TopologicalSorter(self.predecessors()).prepare()
        except CycleError as exc:
            raise GraphCycleError(list(exc.args[1])) from exc

    def static_order(self) -> list[str]:
        """A single linear topological order."""
        try:
            return list(TopologicalSorter(self.predecessors()).static_order())
        except CycleError as exc:
            raise GraphCycleError(list(exc.args[1])) from exc

    def waves(self) -> Iterator[tuple[str, ...]]:
        """Yield successive frontiers: each tuple is a maximal set of nodes whose
        dependencies are all satisfied, i.e. one concurrent dispatch batch."""
        ts = TopologicalSorter(self.predecessors())
        try:
            ts.prepare()
        except CycleError as exc:
            raise GraphCycleError(list(exc.args[1])) from exc
        while ts.is_active():
            ready = tuple(ts.get_ready())
            yield ready
            ts.done(*ready)
