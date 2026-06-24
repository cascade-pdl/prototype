"""Runner vocabulary, per-node config, and deployment config."""

from __future__ import annotations

from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Any, Mapping, Self, Type

from cascade.model.runner_kinds import RunnerKind


class RunnerOverrides(ABC):
    """Per-ref runner config

    * each ref can override a runner configuration
    * at runtime, runner ref config and runner config are merged
    * not all runners can be configured on a per ref basis
    * see the registry below

    loaded from pipeline.refs[].runner_config
    """

    @classmethod
    @abstractmethod
    def decode(cls, raw: dict[str, Any]) -> Self: ...


@dataclass
class SubprocessOverride(RunnerOverrides):
    memory: int | None = None
    cpu: int | None = None

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        return cls(**raw)


@dataclass
class DockerOverride(RunnerOverrides):
    no_pull: bool | None = None
    memory: int | None = None
    cpu: int | None = None

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        return cls(**raw)


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
