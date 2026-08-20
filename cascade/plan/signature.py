"""Signature artifacts: the derived I/O of a runnable.

Pure data — crosses the authoring -> execution boundary. The executor reads it to build
bindings; the node reads those bindings to load and store data procedurally.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from cascade.types import IoConfig, TypeError_, TypeExpr

__all__ = ["Port", "Signature", "TypeExpr", "TypeError_"]  # re-exported: callers import both


@dataclass(frozen=True)
class Port:
    """A resolved port: what flows through it, and what the *container* wants it as.

    ``config`` is carried **whole** rather than having its fields cherry-picked. An earlier
    version copied only ``encoding``, which is how ``mapping`` came to be silently dropped
    at compile time: adding a field to ``IoConfig`` then meant editing four types and
    remembering every one. Carrying the object means ``mapping`` and a future ``transform``
    arrive for free.

    Type and config are deliberately not comparable together. The store holds canonical
    JSON for everything structured, so ``encoding`` says only what a particular tool expects
    on its local disk — two ports with different encodings are perfectly compatible, and
    nothing in ``config`` may play a part in type compatibility.

    That is why ``accepts`` stays on ``TypeExpr`` rather than moving here: a comparison
    site has to reach for ``.type`` explicitly, so it cannot accidentally start comparing
    encodings. Keeping type and encoding in one object at the same time makes them
    impossible to drift apart, which a pair of parallel dicts keyed by port name would not.
    """

    type: TypeExpr
    config: IoConfig = field(default_factory=IoConfig)

    @property
    def encoding(self):
        """Convenience: the encoding is the field of ``config`` read most often."""
        return self.config.encoding

    def encode(self) -> dict[str, Any]:
        return {"type": self.type.encode(), "config": self.config.encode()}

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> "Port":
        return cls(
            type=TypeExpr.decode(raw["type"]),
            config=IoConfig.decode(raw.get("config", {})),
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