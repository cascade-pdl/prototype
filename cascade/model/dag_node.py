from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Self

from cascade.model.dependency import Dependency


@dataclass
class DagNode:
    """
    A node in a dag.

    ``runs`` names the runnable this node executes — a ref *or* a dag (they are
    interchangeable by name), defaulting to the node's own name. It is not
    restricted to refs, which is why the field is ``runs`` and not ``ref``.
    ``scatter`` names an input port to fan out over (one instance per item).
    ``args`` are static kwargs.
    """

    name: str
    runs: str | None = None
    args: dict[str, Any] = field(default_factory=dict)
    scatter: str | None = None
    depends_on: list[Dependency] = field(default_factory=list)

    @property
    def runnable_name(self) -> str:
        return self.runs or self.name

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        return cls(
            name=raw["name"],
            runs=raw.get("runs"),
            args=dict(raw.get("args", {})),
            scatter=raw.get("scatter"),
            depends_on=[Dependency.decode(d) for d in raw.get("depends_on", [])],
        )

    def encode(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "runs": self.runs,
            "args": dict(self.args),
            "scatter": self.scatter,
            "depends_on": [d.encode() for d in self.depends_on],
        }
