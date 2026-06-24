from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Self

from cascade.model.types import IoDecl, TypesSection
from cascade.model.dag import Dag
from cascade.model.refs import Ref


@dataclass
class Pipeline:
    """A pipeline is a namespace of runnables (refs and dags) plus a declared
    ``entrypoint`` naming the one to run. The entrypoint may be a ref (run a unit
    in isolation) or a dag (run a graph) — they are interchangeable by name."""

    entrypoint: str
    refs: list[Ref] = field(default_factory=list)
    dags: list[Dag] = field(default_factory=list)
    types: TypesSection = field(default_factory=TypesSection)
    input: list[IoDecl] = field(default_factory=list)

    def find_ref(self, name: str) -> Ref | None:
        return next((r for r in self.refs if r.name == name), None)

    def find_dag(self, name: str) -> Dag | None:
        return next((d for d in self.dags if d.name == name), None)

    def find(self, name: str) -> Ref | Dag | None:
        """Resolve a runnable by name across both refs and dags."""
        return self.find_ref(name) or self.find_dag(name)

    def find_input(self, name: str) -> IoDecl | None:
        return next((i for i in self.input if i.name == name), None)

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        return cls(
            entrypoint=raw["entrypoint"],
            refs=[Ref.decode(r) for r in raw.get("refs", [])],
            dags=[Dag.decode(d) for d in raw.get("dags", [])],
            types=TypesSection.decode(raw.get("types", {})),
            input=[IoDecl.decode(i) for i in raw.get("input", [])],
        )
