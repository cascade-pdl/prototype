"""Refs runner configuration (the per-ref "how to run it": image, cmd, ...).

Each subclass *is* a runner kind: ``kind`` is a class attribute, so a ref's kind is
derived from its data and the two cannot disagree. ``decode(kind, raw)`` dispatches on
the YAML ``runner:`` tag; ``encode`` is a method (each RefData *is* its data). The Plan
carries it per ref so the executor can construct a runner without the authoring layer.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar, Self, Mapping, Type
from dataclasses import dataclass, field

from cascade.model.runner_kinds import RunnerKind


class RefData(ABC):
    kind: ClassVar[RunnerKind]

    @classmethod
    @abstractmethod
    def decode(cls, raw: dict[str, Any]) -> Self: ...

    @abstractmethod
    def encode(self) -> dict[str, Any]: ...


@dataclass
class RefEcho(RefData):
    kind: ClassVar[RunnerKind] = RunnerKind.echo
    message: str = "message"

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        return cls(message=raw.get("message", "Hello world!"))

    def encode(self) -> dict[str, Any]:
        return {"message": self.message}


@dataclass
class RefSubprocess(RefData):
    kind: ClassVar[RunnerKind] = RunnerKind.subprocess
    cmd: list[str]

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        return cls(cmd=list(raw["cmd"]))

    def encode(self) -> dict[str, Any]:
        return {"cmd": list(self.cmd)}


@dataclass
class RefDocker(RefData):
    kind: ClassVar[RunnerKind] = RunnerKind.docker
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
    cls.kind: cls for cls in (RefEcho, RefDocker, RefSubprocess)
}


def decode(kind: RunnerKind, raw: dict[str, Any]) -> RefData:
    return REF_MAP[kind].decode(raw)
