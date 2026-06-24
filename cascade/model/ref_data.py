"""Refs runners configuration"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Self, Mapping, Type
from dataclasses import dataclass, field

from cascade.model.runner_kinds import RunnerKind


class RefData(ABC):

    @classmethod
    @abstractmethod
    def decode(cls, raw: dict[str, Any]) -> Self: ...


@dataclass
class RefEcho(RefData):
    message: str = "message"

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        return cls(message=raw.get("message", "Hello world!"))


@dataclass
class RefSubprocess(RefData):
    cmd: list[str]

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        return cls(cmd=list(raw["cmd"]))


@dataclass
class RefDocker(RefData):
    image: str
    extra_args: list[str] = field(default_factory=list)

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        return cls(
            image=raw["image"],
            extra_args=raw.get("extra_args", []),
        )


REF_MAP: Mapping[RunnerKind, Type[RefData]] = {
    RunnerKind.echo: RefEcho,
    RunnerKind.docker: RefDocker,
    RunnerKind.subprocess: RefSubprocess,
}


def decode(kind: RunnerKind, raw: dict[str, Any]) -> RefData:
    return REF_MAP[kind].decode(raw)
