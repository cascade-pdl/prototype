from typing import Any, Self

from dataclasses import dataclass


@dataclass
class Dependency:
    """One incoming edge of a dag node"""

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
            as_=raw.get("as"),
            mode=raw.get("mode", "single"),
            merge=raw.get("merge", "concat"),
        )
