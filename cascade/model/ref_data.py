"""Refs runner configuration (the per-ref "how to run it": image, cmd, ...).

This is one half of a ref's run config; the other is RunnerOverrides. Both are
tagged by RunnerKind. ``decode(kind, raw)`` dispatches on the kind; ``encode`` is a
method (each RefData *is* its data). The pair is what the Plan carries per ref so
the executor can construct a runner without the authoring layer.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Self, Mapping, Type
from dataclasses import dataclass, field

from cascade.model.runner_kinds import RunnerKind


class RefData(ABC):

    @classmethod
    @abstractmethod
    def decode(cls, raw: dict[str, Any]) -> Self: ...

    @abstractmethod
    def encode(self) -> dict[str, Any]: ...


@dataclass
class RefEcho(RefData):
    message: str = "message"

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        return cls(message=raw.get("message", "Hello world!"))

    def encode(self) -> dict[str, Any]:
        return {"message": self.message}


@dataclass
class RefSubprocess(RefData):
    cmd: list[str]

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        return cls(cmd=list(raw["cmd"]))

    def encode(self) -> dict[str, Any]:
        return {"cmd": list(self.cmd)}


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

    def encode(self) -> dict[str, Any]:
        return {"image": self.image, "extra_args": list(self.extra_args)}


REF_MAP: Mapping[RunnerKind, Type[RefData]] = {
    RunnerKind.echo: RefEcho,
    RunnerKind.docker: RefDocker,
    RunnerKind.subprocess: RefSubprocess,
}


def decode(kind: RunnerKind, raw: dict[str, Any]) -> RefData:
    return REF_MAP[kind].decode(raw)
