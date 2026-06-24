from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Self


@dataclass
class Dependency:
    """One incoming edge of a dag node."""

    node: str
    field: str | None = None
    as_: str | None = None
    mode: str = "single"  # single | gather
    merge: str = "concat"  # concat | dict | latest

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
            mode=raw.get("mode", "single"),
            merge=raw.get("merge", "concat"),
        )

    def encode(self) -> dict[str, Any]:
        return {
            "node": self.node,
            "field": self.field,
            "as": self.as_,
            "mode": self.mode,
            "merge": self.merge,
        }
