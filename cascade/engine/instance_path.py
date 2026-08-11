"""``InstancePath`` — one identity, two renderings.

An instance's position in a run is inherently a path: run, then dag, node, fan
index, nested dag, node, and so on. That is also exactly the shape of a store
scope tuple, so the two are the same fact rather than parallel bookkeeping:
``scope`` feeds ``StoreConfig.subscope`` to narrow a store to this instance's slot,
and ``str()`` renders the same segments for logs, docker ``--name`` and ECS tags.

Because the path is composed by the executor and handed down pre-applied, an
instance never computes it. That is what keeps containers globally depth-blind: an
instance knows its own slot and its siblings' names, but not whether its dag is the
root or nested four levels down — which is what lets a dag be reused at any depth.

Frozen, so a parent's path cannot be mutated by a child's construction.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class InstancePath:
    segments: tuple[str, ...] = ()

    @classmethod
    def root(cls, run_id: str) -> "InstancePath":
        """The path for a run. Every scope descends from here, so two runs never
        collide in the store."""
        return cls((run_id,))

    def child(self, *names: str) -> "InstancePath":
        """Descend by one or more named segments (a dag, or a node within it)."""
        for name in names:
            if not name:
                raise ValueError("path segment must not be empty")
            if "/" in name:
                raise ValueError(f"path segment must not contain '/': {name!r}")
        return InstancePath(self.segments + names)

    def lane(self, index: int) -> "InstancePath":
        """Descend into fan lane ``index``. Named distinctly from ``child`` because
        a lane is a different kind of descent: same node, one element."""
        if index < 0:
            raise ValueError(f"lane index must not be negative: {index}")
        return InstancePath(self.segments + (str(index),))

    @property
    def scope(self) -> tuple[str, ...]:
        """The store scope for this instance — pass to ``StoreConfig.subscope``."""
        return self.segments

    @property
    def parent(self) -> "InstancePath":
        if not self.segments:
            raise ValueError("the empty path has no parent")
        return InstancePath(self.segments[:-1])

    @property
    def name(self) -> str:
        """The final segment — the node or lane this path identifies."""
        return self.segments[-1] if self.segments else ""

    def __str__(self) -> str:
        return "/".join(self.segments)

    def __len__(self) -> int:
        return len(self.segments)