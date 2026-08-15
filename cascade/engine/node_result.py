"""``NodeResult`` — what a node produced, as the dag runner remembers it.

The dag runner's working memory, keyed by node name. It exists because fan is
discovered at runtime rather than recorded in the plan: after node ``each`` has run,
something has to know it produced twelve output sets rather than one, or the gather
edge that consumes it cannot be resolved. ``elaborate.py`` computed that and threw it
away, deliberately, so the runner rebuilds it as it goes.

Scopes are **relative to the dag's own slot**, matching the reader store handed to
each instance — so a result scope drops straight into an ``InputBinding``.

``fanned`` is explicit rather than inferred from ``len(scopes)``. A node that ran once
is depth 0; a scatter that happened to produce exactly one element is depth 1. Collapse
those and you mis-resolve every width-1 fan — a bug that only shows up on small
datasets, which is the worst kind.

Likely to merge with ``CollectionDescriptor`` (item 1.5): a fanned result is count plus
element scopes held in memory, which is nearly what a descriptor is on disk.
"""
from __future__ import annotations

from dataclasses import dataclass


class NodeResultError(Exception):
    """A result was used in a way its shape does not support."""


@dataclass(frozen=True)
class NodeResult:
    scopes: tuple[tuple[str, ...], ...]
    fanned: bool

    @classmethod
    def single(cls, scope: tuple[str, ...]) -> "NodeResult":
        return cls((tuple(scope),), False)

    @classmethod
    def fan(cls, scopes: tuple[tuple[str, ...], ...]) -> "NodeResult":
        return cls(tuple(tuple(s) for s in scopes), True)

    @property
    def scope(self) -> tuple[str, ...]:
        """The single output scope. Raises if this result is fanned — the caller
        must decide how to collapse N before asking for one."""
        if self.fanned:
            raise NodeResultError(
                f"result is fanned over {self.width} lanes; use scopes or gather it"
            )
        return self.scopes[0]

    @property
    def width(self) -> int:
        return len(self.scopes)