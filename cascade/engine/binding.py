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

Encoding travels with the binding rather than being looked up by the instance,
because the engine is the only party that knows it (it is declared in the pipeline).
Until port encodings are persisted in ``Signature`` the executor cannot populate this
faithfully, so it defaults to JSON; the field exists now so that code written against
it does not change when that lands.

These cross into containers via env, so everything here is JSON-encodable.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from cascade.model.types import DataFormat


@dataclass(frozen=True)
class InputBinding:
    """One input port, resolved to a location in the reader (dag-scoped) store."""

    port: str
    scope: tuple[str, ...]
    key: str
    encoding: DataFormat = DataFormat.json

    def encode(self) -> dict[str, Any]:
        return {
            "port": self.port,
            "scope": list(self.scope),
            "key": self.key,
            "encoding": self.encoding.value,
        }

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> "InputBinding":
        return cls(
            port=raw["port"],
            scope=tuple(raw["scope"]),
            key=raw["key"],
            encoding=DataFormat(raw.get("encoding", DataFormat.json.value)),
        )


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