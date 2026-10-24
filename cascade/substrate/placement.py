"""Placement: which backend each ref runs on.

The first point where a compiled plan meets a deployment. Each ref states what it needs
(``resources``, ``requires``) and never where it runs; the deployment lists backends in
order. A ref goes to the **first** backend that can run its kind, whose ``max`` covers
its resources and whose ``provides`` covers its ``requires``. Order is the priority, so
cheap backends go first and expensive ones only take what does not fit before them.

Placement is computed once, before anything runs, and every problem is reported together.
The result is data — a ``PlacementMap`` — so it can be printed (``cascade placement``),
stored beside the plan, and later shipped to a remote coordinator, which reads it rather
than placing again.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator, Self, Sequence

from cascade.model.runner_kinds import RunnerKind
from cascade.plan.plan import Plan
from cascade.plan.run_config import RunConfig
from cascade.substrate.backends import Backend


PLACEMENT_VERSION = 1


class PlacementError(Exception):
    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__("; ".join(errors))


@dataclass(frozen=True)
class Placement:
    ref: str
    backend: str

    def encode(self) -> dict[str, Any]:
        return {"ref": self.ref, "backend": self.backend}

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        return cls(ref=raw["ref"], backend=raw["backend"])


@dataclass(frozen=True)
class PlacementMap:
    placements: tuple[Placement, ...] = ()

    def __post_init__(self) -> None:
        seen: set[str] = set()
        for p in self.placements:
            if p.ref in seen:
                raise PlacementError([f"ref {p.ref!r} is placed twice"])
            seen.add(p.ref)

    def __iter__(self) -> Iterator[Placement]:
        return iter(self.placements)

    def backend_of(self, ref: str) -> str | None:
        return next((p.backend for p in self.placements if p.ref == ref), None)

    def encode(self) -> dict[str, Any]:
        return {
            "version": PLACEMENT_VERSION,
            "placements": [p.encode() for p in self.placements],
        }

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        version = raw.get("version")
        if version != PLACEMENT_VERSION:
            raise PlacementError(
                [f"placement version {version!r} != supported {PLACEMENT_VERSION!r}"]
            )
        return cls(placements=tuple(Placement.decode(p) for p in raw["placements"]))


def skipped_because(backend: Backend, config: RunConfig) -> list[str]:
    """Why ``backend`` cannot take this ref; empty if it can."""
    if not backend.runs(config.runner):
        return [f"a {backend.kind.value} backend cannot run a {config.runner.value} ref"]
    reasons = []
    if backend.max is not None and config.resources is not None:
        reasons += backend.max.shortfalls(config.resources)
    missing = set(config.requires) - (backend.provides or set())
    if missing and backend.provides is not None:
        reasons.append(f"does not provide {sorted(missing)}")
    return reasons


@dataclass(frozen=True)
class Decision:
    """One ref's placement and, for ``--explain``, why earlier backends were passed over."""

    ref: str
    backend: str | None
    skipped: dict[str, list[str]]

    def encode(self, explain: bool = False) -> dict[str, Any]:
        out: dict[str, Any] = {"ref": self.ref, "backend": self.backend}
        if explain:
            out["skipped"] = self.skipped
        return out


def decide(plan: Plan, backends: Sequence[Backend]) -> list[Decision]:
    """First fit for every placed ref, in plan order. Echo refs are never placed."""
    decisions = []
    for ref, config in plan.run_config.items():
        if config.runner is RunnerKind.echo:
            continue
        skipped: dict[str, list[str]] = {}
        chosen = None
        if config.resources is None:
            skipped["*"] = [f"a {config.runner.value} ref must state its resources"]
        elif not backends:
            skipped["*"] = ["the deployment lists no backends"]
        else:
            for backend in backends:
                reasons = skipped_because(backend, config)
                if not reasons:
                    chosen = backend.name
                    break
                skipped[backend.name] = reasons
        decisions.append(Decision(ref=ref, backend=chosen, skipped=skipped))
    return decisions


def place_refs(plan: Plan, backends: Sequence[Backend]) -> PlacementMap:
    """Every placed ref's backend, or a ``PlacementError`` naming every ref that fits
    nowhere."""
    decisions = decide(plan, backends)
    errors = [
        f"ref {d.ref!r} fits no backend: "
        + "; ".join(f"{name}: {', '.join(why)}" for name, why in d.skipped.items())
        for d in decisions
        if d.backend is None
    ]
    if errors:
        raise PlacementError(errors)
    return PlacementMap(tuple(Placement(d.ref, d.backend) for d in decisions))
