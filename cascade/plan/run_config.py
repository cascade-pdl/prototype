"""RunConfig: how to run one ref, as carried in the Plan.

A ref's signature says *what* it consumes/produces; its RunConfig says *how* to
launch it — the kind-specific RefData (image, cmd; the runner kind is derived from
it), the resources it needs, the capabilities it requires, and the named limit it
counts against. This is the ref-only
map (dags have no run config — they are composition): without it the executor knows a node runs ``detect`` but not that ``detect`` is a
docker ref with image X.

It is a strict, fully-resolved record (the author's settled choice), not a
deployment concern — where it runs is placement's answer, kept beside the
Plan, never in it. The ``runner`` tag is the one extensible vocabulary the Plan
carries, and even it is a recorded fact, not a flexibility point.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from cascade.model.runner_kinds import RunnerKind
from cascade.model.ref_data import RefData, decode as decode_ref_data
from cascade.model.refs import Ref
from cascade.model.resources import Resources


@dataclass
class RunConfig:
    config: RefData
    resources: Resources | None = None
    """Required on docker and subprocess refs (compile checks); ``None`` only for echo."""
    requires: tuple[str, ...] = ()
    limit: str | None = None
    """The class of work this ref is (a label); its capacity is the deployment's."""

    @property
    def runner(self) -> RunnerKind:
        return self.config.kind

    @classmethod
    def from_ref(cls, ref: Ref) -> "RunConfig":
        return cls(
            config=ref.config,
            resources=ref.resources,
            requires=ref.requires,
            limit=ref.limit,
        )

    def encode(self) -> dict[str, Any]:
        return {
            "runner": self.runner.value,
            "config": self.config.encode(),
            "resources": self.resources.encode() if self.resources is not None else None,
            "requires": list(self.requires),
            "limit": self.limit,
        }

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> "RunConfig":
        kind = RunnerKind(raw["runner"])
        resources = raw.get("resources")
        return cls(
            config=decode_ref_data(kind, raw["config"]),
            resources=Resources.decode(resources) if resources is not None else None,
            requires=tuple(raw.get("requires") or ()),
            limit=raw.get("limit"),
        )
