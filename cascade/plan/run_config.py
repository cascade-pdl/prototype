"""RunConfig: how to run one ref, as carried in the Plan.

A ref's signature says *what* it consumes/produces; its RunConfig says *how* to
launch it — the runner kind plus the kind-specific RefData (image, cmd) and
optional RunnerOverrides (cpu/memory). This is the ref-only map (dags have no run
config — they are composition) that closes PLAN_SPEC issue #4: without it the
executor knows a node runs ``detect`` but not that ``detect`` is a docker ref with
image X.

It is a strict, fully-resolved record (the author's settled choice), not a
deployment concern — the deployment defaults are a *third* leg merged at spawn
time, not here. The ``runner`` tag is the one extensible vocabulary the Plan
carries, and even it is a recorded fact, not a flexibility point.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from cascade.model.runner_kinds import RunnerKind
from cascade.model.ref_data import RefData, decode as decode_ref_data
from cascade.model.runner_overrides import RunnerOverrides, parse as parse_overrides
from cascade.model.refs import Ref


@dataclass
class RunConfig:
    runner: RunnerKind
    config: RefData
    overrides: RunnerOverrides | None = None

    @classmethod
    def from_ref(cls, ref: Ref) -> "RunConfig":
        return cls(runner=ref.runner, config=ref.config, overrides=ref.overrides)

    def encode(self) -> dict[str, Any]:
        return {
            "runner": self.runner.value,
            "config": self.config.encode(),
            "overrides": self.overrides.encode() if self.overrides is not None else None,
        }

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> "RunConfig":
        kind = RunnerKind(raw["runner"])
        return cls(
            runner=kind,
            config=decode_ref_data(kind, raw["config"]),
            overrides=parse_overrides(kind, raw.get("overrides")),
        )
