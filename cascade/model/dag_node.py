from dataclasses import dataclass, field
from typing import Any, Self

from cascade.model.dependency import Dependency
from cascade.model.runnable import Runnable


@dataclass
class DagNode:
    """
    A node in a dag.

    ``runnable`` names the ref it runs (defaults to the node's own key in the dag).
    ``scatter`` names an upstream collection field to fan out over (one instance per item).
    ``args`` are static kwargs.
    """

    name: str
    ref: str | None = None
    args: dict[str, Any] = field(default_factory=dict)
    scatter: str | None = None
    depends_on: list[Dependency] = field(default_factory=list)

    @property
    def ref_name(self) -> str:
        return self.ref or self.name

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        return cls(
            name=raw["name"],
            ref=raw.get("ref"),
            args=dict(raw.get("args", {})),
            scatter=raw.get("scatter"),
            depends_on=[Dependency.decode(d) for d in raw.get("depends_on", [])],
        )
