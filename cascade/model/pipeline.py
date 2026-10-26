from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Self

from cascade.types import IoDecl, TypesSection
from cascade.model.dag import Dag
from cascade.model.refs import Ref


@dataclass
class Pipeline:
    """A pipeline is a namespace of runnables (refs and dags) plus a declared
    ``entrypoint`` naming the one to run. The entrypoint may be a ref (run a unit
    in isolation) or a dag (run a graph) — they are interchangeable by name. A ref
    entrypoint is compiled into an implicit one-node dag named ``main``.

    ``limits`` caps classes of work wherever they run: ``{bulky: 2, portal_api: 5}``. A
    ref names its class (``limit: bulky``) and this says how many run at once. Both halves
    are the pipeline author's, who knows what the work does to what it touches, so a
    pipeline is self-contained and an undeclared label is caught at compile. A backend's
    own cap is its ``concurrency``, in the deployment."""

    entrypoint: str
    refs: list[Ref] = field(default_factory=list)
    dags: list[Dag] = field(default_factory=list)
    types: TypesSection = field(default_factory=TypesSection)
    input: list[IoDecl] = field(default_factory=list)
    limits: dict[str, int] = field(default_factory=dict)

    def find_ref(self, name: str) -> Ref | None:
        return next((r for r in self.refs if r.name == name), None)

    def find_dag(self, name: str) -> Dag | None:
        return next((d for d in self.dags if d.name == name), None)

    def find(self, name: str) -> Ref | Dag | None:
        """Resolve a runnable by name across both refs and dags."""
        return self.find_ref(name) or self.find_dag(name)

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        return cls(
            entrypoint=raw["entrypoint"],
            refs=[Ref.decode(r) for r in raw.get("refs", [])],
            dags=[Dag.decode(d) for d in raw.get("dags", [])],
            types=TypesSection.decode(raw.get("types", {})),
            input=[IoDecl.decode(i) for i in raw.get("input", [])],
            limits=decode_limits(raw.get("limits") or {}),
        )


def decode_limits(raw: Any) -> dict[str, int]:
    """``limits:`` as label -> positive number of refs at once."""
    if not isinstance(raw, dict):
        raise ValueError(f"`limits` maps a label to a number of tasks, got {raw!r}")
    bad = {
        label: n for label, n in raw.items()
        if isinstance(n, bool) or not isinstance(n, int) or n < 1
    }
    if bad:
        raise ValueError(f"limits must be positive integers: {bad}")
    return {str(label): n for label, n in raw.items()}