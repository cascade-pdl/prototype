"""Signature artifacts: the derived I/O of a runnable.

Pure data — crosses the authoring -> execution boundary. The executor reads it to build
bindings; the node reads those bindings to load and store data procedurally.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from cascade.model.types import DataFormat, TypeError_, TypeExpr

__all__ = ["Port", "Signature", "TypeExpr", "TypeError_"]  # re-exported: callers import both


@dataclass(frozen=True)
class Port:
    """A resolved port: what flows through it, and what the *container* wants it as.

    The two are separate concerns and deliberately not comparable together. The store
    holds canonical JSON for everything structured, so ``encoding`` says only what a
    particular tool expects on its local disk — which means two ports with different
    encodings are perfectly compatible, and encoding must play no part in type
    compatibility.

    That is why ``accepts`` stays on ``TypeExpr`` rather than moving here: a comparison
    site has to reach for ``.type`` explicitly, so it cannot accidentally start comparing
    encodings. Keeping type and encoding in one object at the same time makes them
    impossible to drift apart, which a pair of parallel dicts keyed by port name would not.
    """

    type: TypeExpr
    encoding: DataFormat = DataFormat.json

    def encode(self) -> dict[str, Any]:
        return {"type": self.type.encode(), "encoding": self.encoding.value}

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> "Port":
        return cls(
            type=TypeExpr.decode(raw["type"]),
            encoding=DataFormat(raw.get("encoding", DataFormat.json.value)),
        )


@dataclass
class Signature:
    """Resolved I/O of a runnable, keyed by port name."""

    inputs: dict[str, Port]
    outputs: dict[str, Port]

    def encode(self) -> dict[str, Any]:
        return {
            "inputs": {k: v.encode() for k, v in self.inputs.items()},
            "outputs": {k: v.encode() for k, v in self.outputs.items()},
        }

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> "Signature":
        return cls(
            inputs={k: Port.decode(v) for k, v in raw["inputs"].items()},
            outputs={k: Port.decode(v) for k, v in raw["outputs"].items()},
        )