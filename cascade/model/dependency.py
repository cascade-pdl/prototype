from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Self


@dataclass
class Dependency:
    """One incoming edge of a dag node.

    There is no ``mode``. A fan is exactly one node deep — it opens at a node's
    ``scatter`` and closes at that same node's boundary, where the fan runner gathers
    the lanes — so an edge never has a choice to make: consuming a fanned node always
    yields the gathered collection, consuming an ordinary node yields its output as
    declared. Multi-stage per-element work is expressed by wrapping those stages in a
    subdag and scattering *that*, which states the boundary explicitly instead of
    letting a fan propagate silently across edges.

    ``merge`` moved to ``DagNode`` for the same reason: gathering happens once, at the
    producing node, so the policy belongs to the producer rather than to each consumer.
    """

    node: str
    field: str | None = None
    as_: str | None = None

    @property
    def is_input(self) -> bool:
        # the default name for a dag/subdag input
        return self.node == "$input"

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        return cls(
            node=raw["node"],
            field=raw.get("field"),
            as_=raw.get("as"),  # 'as' is a keyword; stored as as_
        )

    def encode(self) -> dict[str, Any]:
        return {
            "node": self.node,
            "field": self.field,
            "as": self.as_,
        }