"""Signature artifacts: the derived I/O of a runnable.

Pure data — crosses the authoring -> execution boundary (the node uses signatures
to load and store data procedurally). No dependency on the model or the passes.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from cascade.model.types import TypeError_, TypeExpr

__all__ = ["Signature", "TypeExpr", "TypeError_"]  # re-exported: callers import both


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