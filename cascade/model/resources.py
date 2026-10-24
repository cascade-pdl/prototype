"""What a piece of work needs to run: cpu, memory, gpu.

The same record serves both sides of placement. On a ref it is a set of *minimums* — the
work needs at least this much, and gets exactly this much on any substrate that can cap
it. On a deployment backend it is a set of *maximums* — the largest task that backend
accepts, a policy the deployment states rather than a hardware description. Placement is
then one comparison: ``backend.max.shortfalls(ref.resources)`` is empty.

Units are fixed so a number means the same everywhere: ``cpu`` in vCPU (a float, so
``0.5`` is half a CPU; each substrate converts — docker ``--cpus``, ECS 1024 per vCPU),
``memory`` as a quantity with a unit (``4GiB``, ``512MiB``, ``2GB``) held in MiB, ``gpu``
as a count. A bare number for memory is rejected: ``4096`` could be MB, MiB or bytes.
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any, Self


class ResourcesError(ValueError):
    """A resources block that does not parse."""


_UNITS_MIB = {
    "KiB": 1 / 1024,
    "MiB": 1,
    "GiB": 1024,
    "TiB": 1024 * 1024,
    "KB": 1000 / 1024 ** 2,
    "MB": 1000 ** 2 / 1024 ** 2,
    "GB": 1000 ** 3 / 1024 ** 2,
    "TB": 1000 ** 4 / 1024 ** 2,
}
_QUANTITY = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([KMGT]i?B)\s*$")


def parse_memory(raw: Any) -> int:
    """A memory quantity in MiB, rounded up: asking for 1.5MB must not yield less."""
    if not isinstance(raw, str):
        raise ResourcesError(
            f"memory {raw!r} needs a unit, e.g. '4GiB' or '512MiB'"
        )
    match = _QUANTITY.match(raw)
    if match is None or match.group(2) not in _UNITS_MIB:
        raise ResourcesError(
            f"memory {raw!r} is not a quantity; use e.g. '4GiB', '512MiB' or '2GB'"
        )
    mib = math.ceil(float(match.group(1)) * _UNITS_MIB[match.group(2)])
    if mib <= 0:
        raise ResourcesError(f"memory {raw!r} must be positive")
    return mib


def format_memory(mib: int) -> str:
    """The shortest exact spelling, so an encoded block reads like a written one."""
    if mib % 1024 == 0:
        return f"{mib // 1024}GiB"
    return f"{mib}MiB"


@dataclass(frozen=True)
class Resources:
    cpu: float
    """vCPU."""
    memory: int
    """MiB."""
    gpu: int = 0

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        if not isinstance(raw, dict):
            raise ResourcesError(f"resources must be a mapping, got {raw!r}")
        unknown = set(raw) - {"cpu", "memory", "gpu"}
        if unknown:
            raise ResourcesError(f"unknown resource(s): {sorted(unknown)}")
        missing = {"cpu", "memory"} - set(raw)
        if missing:
            raise ResourcesError(f"resources need {sorted(missing)}")
        cpu, gpu = raw["cpu"], raw.get("gpu", 0)
        if isinstance(cpu, bool) or not isinstance(cpu, (int, float)) or cpu <= 0:
            raise ResourcesError(f"cpu {cpu!r} must be a positive number of vCPU")
        if isinstance(gpu, bool) or not isinstance(gpu, int) or gpu < 0:
            raise ResourcesError(f"gpu {gpu!r} must be a whole count")
        return cls(cpu=float(cpu), memory=parse_memory(raw["memory"]), gpu=gpu)

    def encode(self) -> dict[str, Any]:
        cpu: float | int = int(self.cpu) if self.cpu.is_integer() else self.cpu
        return {"cpu": cpu, "memory": format_memory(self.memory), "gpu": self.gpu}

    def shortfalls(self, need: Resources) -> list[str]:
        """Where ``need`` exceeds ``self`` (read as maximums); empty if it fits."""
        out = []
        if need.cpu > self.cpu:
            out.append(f"cpu {need.cpu:g} > max {self.cpu:g}")
        if need.memory > self.memory:
            out.append(
                f"memory {format_memory(need.memory)} > max {format_memory(self.memory)}"
            )
        if need.gpu > self.gpu:
            out.append(f"gpu {need.gpu} > max {self.gpu}")
        return out
