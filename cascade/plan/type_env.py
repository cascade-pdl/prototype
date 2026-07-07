"""The resolved type environment: structures keyed by name, plus the set of
built-in primitive type names.

This artifact crosses into the execution environment, where a node uses it to
deserialise a payload, validate it against its declared structure, and read/write
the data plane. It is NOT consumed by authoring-time edge validation — edges are
checked by type-expression name + arity, which needs no registry (see plan.validate).

The subtype check (walking ``extends``) is provided for the runtime payload
validator; the authoring layer does not call it.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from cascade.model.pipeline import Pipeline
from cascade.model.types import Structure


# Built-in, structureless type names that are valid but not records.
DEFAULT_PRIMITIVES: frozenset[str] = frozenset(
    {"io.Image", "float", "int", "string", "bool"}
)


@dataclass
class TypeEnv:
    structures: dict[str, Structure] = field(default_factory=dict)
    primitives: frozenset[str] = DEFAULT_PRIMITIVES

    def is_defined(self, base: str) -> bool:
        """Is this base type name known — a declared structure or a primitive?"""
        return base in self.structures or base in self.primitives

    def is_subtype(self, a: str, b: str) -> bool:
        """Is base type ``a`` a subtype of ``b``, walking ``extends``? (Runtime use.)"""
        seen: set[str] = set()
        cur: str | None = a
        while cur is not None and cur not in seen:
            if cur == b:
                return True
            seen.add(cur)
            s = self.structures.get(cur)
            cur = s.extends if s else None
        return False

    def encode(self) -> dict[str, Any]:
        return {
            "structures": {name: s.encode() for name, s in self.structures.items()},
            "primitives": sorted(self.primitives),
        }

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> "TypeEnv":
        return cls(
            structures={
                name: Structure.decode(s) for name, s in raw["structures"].items()
            },
            primitives=frozenset(raw["primitives"]),
        )


def resolve_types(pipeline: Pipeline, primitives: frozenset[str] = DEFAULT_PRIMITIVES) -> TypeEnv:
    """Turn the pipeline's declared structures into a name-keyed registry."""
    return TypeEnv(
        structures={s.name: s for s in pipeline.types.structures},
        primitives=primitives,
    )
