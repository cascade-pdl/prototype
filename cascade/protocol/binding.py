"""Bindings — what an instance is told about its *inputs*.

Under the addressing model this project settled on, nothing is staged: an instance
receives a store scoped to its parent dag (so sibling node outputs are visible) and
is told, per port, which sibling scope and key to read. It writes to its own slot
through a separate, instance-scoped store. No copies, no duplicate storage, and the
layout in the store mirrors the dag.

**Inputs only, deliberately.** Outputs need no binding: every instance of a runnable
writes the same ports (they are in ``Signature.outputs``, which the plan already
carries), and *where* they land is already decided by the scope of the writer store.
Nothing about an output varies per instance, so there is nothing to communicate.
Inputs are the opposite — lane 3 reads element 3, and two instances of one ref point
at different producers — which is exactly why they travel per spawn.

``encoding`` and ``depth`` travel with the binding because the engine is the only party
that knows them — both are declared in the pipeline, and re-deriving either inside a
container would mean shipping the plan with every task.

``depth`` is what a ref checks itself against: a port declared depth 0 that receives a
list, or depth 1 that receives a scalar, is a mismatch worth failing on loudly rather
than misinterpreting. It is *not* needed to decide how to read — ``Store.read`` resolves
a collection descriptor whatever the declared shape — but it is what turns a silent
misread into an error, and it is the hook the codec (item 1.7) needs to split a
monolithic non-JSON collection.

Until port encodings are persisted in ``Signature`` the executor cannot populate
``encoding`` faithfully, so it defaults to JSON; the field exists now so that code
written against it does not change when that lands.

These cross into containers via env, so everything here is JSON-encodable.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from cascade.types import DataFormat, IoConfig, TypeExpr


@dataclass(frozen=True)
class InputBinding:
    """One input port, resolved to a location in the reader (dag-scoped) store."""

    port: str
    scope: tuple[str, ...]
    key: str
    type: TypeExpr
    config: IoConfig = field(default_factory=IoConfig)

    @property
    def encoding(self) -> DataFormat:
        return self.config.encoding

    @property
    def depth(self) -> int:
        """Derived, never stored: two fields that can disagree is one too many."""
        return self.type.depth

    def encode(self) -> dict[str, Any]:
        return {
            "port": self.port,
            "scope": list(self.scope),
            "key": self.key,
            "type": self.type.encode(),
            "config": self.config.encode(),
        }

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> "InputBinding":
        return cls(
            port=raw["port"],
            scope=tuple(raw["scope"]),
            key=raw["key"],
            type=TypeExpr.decode(raw["type"]),
            config=IoConfig.decode(raw.get("config", {})),
        )


@dataclass(frozen=True)
class OutputDecl:
    """One output port, as the node needs to know it.

    Not a binding: an output needs no *location*, because the writer store is already
    scoped to this instance's slot. What the node cannot work out for itself is which
    ports exist and what encoding each wants — it has no access to the plan — so that
    much has to travel. Identity and encoding, never scope or key.
    """

    port: str
    type: TypeExpr
    config: IoConfig = field(default_factory=IoConfig)

    @property
    def encoding(self) -> DataFormat:
        return self.config.encoding

    @property
    def depth(self) -> int:
        return self.type.depth

    def encode(self) -> dict[str, Any]:
        return {
            "port": self.port,
            "type": self.type.encode(),
            "config": self.config.encode(),
        }

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> "OutputDecl":
        return cls(
            port=raw["port"],
            type=TypeExpr.decode(raw["type"]),
            config=IoConfig.decode(raw.get("config", {})),
        )


@dataclass(frozen=True)
class OutputDecls:
    """The declared outputs for one instance."""

    outputs: tuple[OutputDecl, ...] = ()

    def decl_for(self, port: str) -> OutputDecl | None:
        return next((o for o in self.outputs if o.port == port), None)

    @property
    def ports(self) -> tuple[str, ...]:
        return tuple(o.port for o in self.outputs)

    def encode(self) -> list[dict[str, Any]]:
        return [o.encode() for o in self.outputs]

    @classmethod
    def decode(cls, raw: list[dict[str, Any]]) -> "OutputDecls":
        return cls(outputs=tuple(OutputDecl.decode(o) for o in raw or ()))


@dataclass(frozen=True)
class InputBindings:
    """The resolved inputs for one instance."""

    inputs: tuple[InputBinding, ...] = ()

    def input_for(self, port: str) -> InputBinding | None:
        for binding in self.inputs:
            if binding.port == port:
                return binding
        return None

    @property
    def ports(self) -> tuple[str, ...]:
        return tuple(b.port for b in self.inputs)

    def encode(self) -> list[dict[str, Any]]:
        return [b.encode() for b in self.inputs]

    @classmethod
    def decode(cls, raw: list[dict[str, Any]]) -> "InputBindings":
        return cls(inputs=tuple(InputBinding.decode(b) for b in raw or ()))