"""A generic directed-graph container — neutral, depends on nothing but stdlib.

Lives at the package root because it is dual-consumed: the plan layer builds and
elaborates over it, and the engine schedules over it. Neither layer owns it.

The structure (nodes, edges, ordering, cycle detection) is domain-free; domain
data lives in the payloads ``N`` (node) and ``E`` (edge). Ordering and cycle
detection delegate to ``graphlib.TopologicalSorter`` (stdlib, purpose-built).

Serialization is structure + payload-by-codec: the graph emits node ids and edge
endpoints itself, and serializes payloads through caller-supplied encode/decode
callables — so the graph never imports the model, and a serialized graph is JSON,
not a pickle. ``waves()`` yields concurrent dispatch batches for the executor.
"""
from __future__ import annotations

from dataclasses import dataclass
from graphlib import CycleError, TopologicalSorter
from typing import Any, Callable, Generic, Iterator, TypeVar

N = TypeVar("N")  # node payload
E = TypeVar("E")  # edge payload


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

    # ------------------------------------------------------ graphlib bridge
    def predecessors(self) -> dict[str, set[str]]:
        """The {node: {predecessors}} view that feeds TopologicalSorter directly."""
        return {n: {e.src for e in self._in[n]} for n in self._nodes}

    def check_acyclic(self) -> None:
        try:
            TopologicalSorter(self.predecessors()).prepare()
        except CycleError as exc:
            raise GraphCycleError(list(exc.args[1])) from exc

    def static_order(self) -> list[str]:
        try:
            return list(TopologicalSorter(self.predecessors()).static_order())
        except CycleError as exc:
            raise GraphCycleError(list(exc.args[1])) from exc

    def waves(self) -> Iterator[tuple[str, ...]]:
        ts = TopologicalSorter(self.predecessors())
        try:
            ts.prepare()
        except CycleError as exc:
            raise GraphCycleError(list(exc.args[1])) from exc
        while ts.is_active():
            ready = tuple(ts.get_ready())
            yield ready
            ts.done(*ready)

    # -------------------------------------------------------- serialization
    def encode(self, enc_node: Callable[[N], Any], enc_edge: Callable[[E], Any]) -> dict[str, Any]:
        return {
            "nodes": {nid: enc_node(p) for nid, p in self._nodes.items()},
            "edges": [
                [e.src, e.dst, enc_edge(e.payload)]
                for nid in self._nodes
                for e in self._out[nid]
            ],
        }

    @classmethod
    def decode(
        cls,
        raw: dict[str, Any],
        dec_node: Callable[[Any], N],
        dec_edge: Callable[[Any], E],
    ) -> "Graph[N, E]":
        g: Graph[N, E] = cls()
        for nid, payload in raw["nodes"].items():
            g.add_node(nid, dec_node(payload))
        for src, dst, ep in raw["edges"]:
            g.add_edge(src, dst, dec_edge(ep))
        return g

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Graph):
            return NotImplemented
        # node payloads and edge lists compare structurally (payloads are dataclasses).
        # build/decode add edges in a deterministic order, so list equality is exact.
        return self._nodes == other._nodes and self._out == other._out
