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
    """A command on the executor's host: the local iteration path.

    ``wrap`` (default true): cascade stages the inputs under a per-instance directory,
    runs ``cmd``, and collects the outputs, so ``cmd`` only reads ``inputs/<port>`` and
    writes ``outputs/<port>`` under ``$CASCADE_ROOT``. ``false`` for a command that speaks
    the protocol itself through the node SDK.
    """

    kind: ClassVar[RunnerKind] = RunnerKind.subprocess
    cmd: list[str]
    wrap: bool = True

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        _reject_unknown("subprocess", raw, {"cmd", "wrap"})
        return cls(cmd=_command("subprocess", "cmd", raw.get("cmd")), wrap=_flag(raw, "wrap"))

    def encode(self) -> dict[str, Any]:
        return {"cmd": list(self.cmd), "wrap": self.wrap}


@dataclass
class RefDocker(RefData):
    """An image, and optionally the command to run in it.

    ``command`` replaces the image's default command (Docker's ``CMD``, ECS's ``command``
    override) and keeps its entrypoint; absent, the image runs as published.

    ``wrap`` (default true): cascade is supplied from outside the image and wraps the
    image's entrypoint + command, staging inputs under ``/cascade/inputs/<port>`` and
    collecting ``/cascade/outputs/<port>``, so the image needs no cascade inside. ``false``
    for an image that speaks the protocol itself (the node SDK installed in it); it then
    runs unwrapped, as published.
    """

    kind: ClassVar[RunnerKind] = RunnerKind.docker
    image: str
    extra_args: list[str] = field(default_factory=list)
    command: list[str] | None = None
    wrap: bool = True

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        _reject_unknown("docker", raw, {"image", "extra_args", "command", "wrap"})
        if not isinstance(raw.get("image"), str) or not raw["image"]:
            raise ValueError("docker config: `image` must be a non-empty string")
        command = raw.get("command")
        return cls(
            image=raw["image"],
            extra_args=raw.get("extra_args", []),
            command=None if command is None else _command("docker", "command", command),
            wrap=_flag(raw, "wrap"),
        )

    def encode(self) -> dict[str, Any]:
        return {
            "image": self.image,
            "extra_args": list(self.extra_args),
            "command": None if self.command is None else list(self.command),
            "wrap": self.wrap,
        }


def _reject_unknown(kind: str, raw: dict[str, Any], known: set[str]) -> None:
    """A misspelt setting (``wrapp: false``) must not be silently dropped: the work would
    run some other way than written."""
    unknown = set(raw) - known
    if unknown:
        raise ValueError(f"{kind} config: unknown field(s) {sorted(unknown)}")


def _command(kind: str, name: str, value: Any) -> list[str]:
    if (
        not isinstance(value, list)
        or not value
        or not all(isinstance(a, str) for a in value)
    ):
        raise ValueError(
            f"{kind} config: `{name}` must be a non-empty list of strings, got {value!r}"
        )
    return list(value)


def _flag(raw: dict[str, Any], name: str) -> bool:
    value = raw.get(name, True)
    if not isinstance(value, bool):
        raise ValueError(f"`{name}` must be true or false, got {value!r}")
    return value


REF_MAP: Mapping[RunnerKind, Type[RefData]] = {
    cls.kind: cls for cls in (RefEcho, RefDocker, RefSubprocess)
}


def decode(kind: RunnerKind, raw: dict[str, Any]) -> RefData:
    return REF_MAP[kind].decode(raw)
