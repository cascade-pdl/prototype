"""Per-ref runner overrides (cpu/memory/no_pull tuning), tagged by RunnerKind.

The second half of a ref's run config. ``parse(kind, raw)`` decodes (and tolerates
None / kinds with no overrides); ``encode`` is a method. Carried in the Plan
alongside RefData so the executor can merge ref config + overrides + deployment
defaults at spawn time.
"""

from __future__ import annotations

from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Any, ClassVar, Mapping, Self, Type

from cascade.model.runner_kinds import RunnerKind


class RunnerOverrides(ABC):
    """Tunables a ref or a deployment may state, merged with the ref winning.

    Some fields are *deployment-only*: a ref naming a path on somebody's laptop, or
    deciding which user a container runs as, is a category error rather than a preference.
    ``deployment_only`` names them, and ``validate`` rejects them in a ref's overrides —
    one config surface with a rule, rather than two parallel mechanisms.
    """

    deployment_only: ClassVar[frozenset[str]] = frozenset()

    """Per-ref runner config. Not every runner kind supports overrides (see the
    registry below); a kind with no entry simply has no overrides."""

    @classmethod
    @abstractmethod
    def decode(cls, raw: dict[str, Any]) -> Self: ...

    @abstractmethod
    def encode(self) -> dict[str, Any]: ...


@dataclass
class DockerOverride(RunnerOverrides):
    deployment_only: ClassVar[frozenset[str]] = frozenset(
        {"aws_credentials_dir", "map_current_user"}
    )

    no_pull: bool | None = None
    memory: int | None = None
    cpu: int | None = None
    aws_credentials_dir: str | None = None
    """Host path to mount at ``~/.aws``. Deployment-only: a *ref* naming a path on
    somebody's laptop is a category error, so ``validate`` rejects it there."""
    map_current_user: bool | None = None
    """Run as the invoking user, so files written to a mounted store are owned by them.
    Deployment-only, for the same reason."""

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        return cls(
            no_pull=raw.get("no_pull"),
            aws_credentials_dir=raw.get("aws_credentials_dir"),
            map_current_user=raw.get("map_current_user"),
            memory=raw.get("memory"),
            cpu=raw.get("cpu"),
        )

    def encode(self) -> dict[str, Any]:
        return {
            "no_pull": self.no_pull,
            "memory": self.memory,
            "cpu": self.cpu,
            "aws_credentials_dir": self.aws_credentials_dir,
            "map_current_user": self.map_current_user,
        }


RUNNER_OVERRIDES: Mapping[RunnerKind, Type[RunnerOverrides]] = {
    RunnerKind.docker: DockerOverride,
}


def parse(kind: RunnerKind, raw: dict[str, Any] | None) -> RunnerOverrides | None:
    if raw is None:
        return None
    if kind not in RUNNER_OVERRIDES:
        return None
    return RUNNER_OVERRIDES[kind].decode(raw)
