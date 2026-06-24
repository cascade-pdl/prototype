from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Self

from cascade.model.dag_node import DagNode
from cascade.model.dependency import Dependency
from cascade.model.types import IoDecl


@dataclass
class Dag:
    """A named subgraph. Its inputs are declared; its outputs are dependencies
    onto its own nodes (the dual of the ``$input`` source used by nodes)."""

    name: str
    nodes: list[DagNode] = field(default_factory=list)
    input: list[IoDecl] = field(default_factory=list)
    output: list[Dependency] = field(default_factory=list)

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        return cls(
            name=raw["name"],
            nodes=[DagNode.decode(n) for n in raw.get("nodes", [])],
            input=[IoDecl.decode(i) for i in raw.get("input", [])],
            output=[Dependency.decode(o) for o in raw.get("output", [])],
        )
