"""Per-ref runner overrides (cpu/memory/no_pull tuning), tagged by RunnerKind.

The second half of a ref's run config. ``parse(kind, raw)`` decodes (and tolerates
None / kinds with no overrides); ``encode`` is a method. Carried in the Plan
alongside RefData so the executor can merge ref config + overrides + deployment
defaults at spawn time.
"""

from __future__ import annotations

from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Any, Mapping, Self, Type

from cascade.model.runner_kinds import RunnerKind


class RunnerOverrides(ABC):
    """Per-ref runner config. Not every runner kind supports overrides (see the
    registry below); a kind with no entry simply has no overrides."""

    @classmethod
    @abstractmethod
    def decode(cls, raw: dict[str, Any]) -> Self: ...

    @abstractmethod
    def encode(self) -> dict[str, Any]: ...


@dataclass
class SubprocessOverride(RunnerOverrides):
    memory: int | None = None
    cpu: int | None = None

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        return cls(memory=raw.get("memory"), cpu=raw.get("cpu"))

    def encode(self) -> dict[str, Any]:
        return {"memory": self.memory, "cpu": self.cpu}


@dataclass
class DockerOverride(RunnerOverrides):
    no_pull: bool | None = None
    memory: int | None = None
    cpu: int | None = None

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        return cls(
            no_pull=raw.get("no_pull"),
            memory=raw.get("memory"),
            cpu=raw.get("cpu"),
        )

    def encode(self) -> dict[str, Any]:
        return {"no_pull": self.no_pull, "memory": self.memory, "cpu": self.cpu}


RUNNER_OVERRIDES: Mapping[RunnerKind, Type[RunnerOverrides]] = {
    RunnerKind.docker: DockerOverride,
    RunnerKind.subprocess: SubprocessOverride,
}


def parse(kind: RunnerKind, raw: dict[str, Any] | None) -> RunnerOverrides | None:
    if raw is None:
        return None
    if kind not in RUNNER_OVERRIDES:
        return None
    return RUNNER_OVERRIDES[kind].decode(raw)
