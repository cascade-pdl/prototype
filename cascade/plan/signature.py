"""Signature artifacts: the derived I/O of a runnable.

Pure data — crosses the authoring -> execution boundary (the node uses signatures
to load and store data procedurally). No dependency on the model or the passes.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class TypeError_(Exception):
    """Problem with a type expression."""


@dataclass(frozen=True)
class TypeExpr:
    """A type expression split into a base type and an array nesting depth.
    ``"Detection[]"`` -> base="Detection", depth=1; ``"float"`` -> depth=0.
    Serializes as its rendered string form."""

    base: str
    depth: int

    @classmethod
    def parse(cls, s: str) -> "TypeExpr":
        depth = 0
        while s.endswith("[]"):
            s, depth = s[:-2], depth + 1
        return cls(s.strip(), depth)

    def render(self) -> str:
        return self.base + "[]" * self.depth

    def as_collection(self) -> "TypeExpr":
        return TypeExpr(self.base, self.depth + 1)

    def element(self) -> "TypeExpr":
        if self.depth < 1:
            raise TypeError_(f"cannot take element of non-collection {self.render()!r}")
        return TypeExpr(self.base, self.depth - 1)

    # serialization is just the string form
    def encode(self) -> str:
        return self.render()

    @classmethod
    def decode(cls, raw: str) -> "TypeExpr":
        return cls.parse(raw)


@dataclass
class Signature:
    """Resolved I/O of a runnable, keyed by port name."""

    inputs: dict[str, TypeExpr]
    outputs: dict[str, TypeExpr]

    def encode(self) -> dict[str, Any]:
        return {
            "inputs": {k: v.encode() for k, v in self.inputs.items()},
            "outputs": {k: v.encode() for k, v in self.outputs.items()},
        }

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> "Signature":
        return cls(
            inputs={k: TypeExpr.decode(v) for k, v in raw["inputs"].items()},
            outputs={k: TypeExpr.decode(v) for k, v in raw["outputs"].items()},
        )
