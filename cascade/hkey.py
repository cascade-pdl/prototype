"""Hierarchical instance keys — the identity of a node instance.

Validated standalone first (see the hkey-proof). An instance is a PATH that
grows one segment per scatter level, which makes nested scatter the general
case rather than a special one:

    ()                                          the single, unscattered instance
    (("images","image-A"),)                     one image, after scatter over images
    (("images","image-A"),("dets","det-A0"))    detection A0 of image A  (nested)

Three operations define the whole scatter model:
  - scatter appends a segment (one child per reported item)
  - carry-through inherits the path unchanged (1:1 with the scattered upstream)
  - gather strips the last segment and groups siblings by their common parent

Single scatter is depth-1; nested scatter is depth-N; they are the same
operation at different depths.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class InstanceKey:
    """A path identifying one node instance. Empty tuple = the unscattered one."""
    segments: tuple[tuple[str, str], ...] = ()

    @property
    def depth(self) -> int:
        return len(self.segments)

    def child(self, axis: str, item: str) -> "InstanceKey":
        """Append a scatter level (used when scattering over ``axis``)."""
        return InstanceKey(self.segments + ((axis, item),))

    def parent(self) -> "InstanceKey":
        """Drop the last scatter level (used by gather to find the collapse target)."""
        if not self.segments:
            raise ValueError("cannot take parent of the root instance key ()")
        return InstanceKey(self.segments[:-1])

    def last_axis(self) -> str | None:
        return self.segments[-1][0] if self.segments else None

    def last_item(self) -> str | None:
        return self.segments[-1][1] if self.segments else None

    def is_ancestor_of(self, other: "InstanceKey") -> bool:
        """True if this path is a prefix of ``other`` (other is nested under self).
        A key is considered an ancestor of itself."""
        return other.segments[: self.depth] == self.segments

    def render(self) -> str:
        """Human-readable: images=image-A/dets=det-A0   (root -> '_root')."""
        if not self.segments:
            return "_root"
        return "/".join(f"{a}={i}" for a, i in self.segments)

    def as_store_fragment(self) -> str:
        """Filesystem-safe path fragment for store keys. Visible lineage on disk
        makes misrouted instances inspectable."""
        if not self.segments:
            return "_root"
        return "/".join(f"{a}={i}".replace(":", "_").replace(" ", "_") for a, i in self.segments)

    def __str__(self) -> str:
        return self.render()

    @classmethod
    def from_render(cls, s: str) -> "InstanceKey":
        """Inverse of render(), for reconstructing a key from run-state JSON."""
        if s == "_root" or not s:
            return cls()
        segs = []
        for part in s.split("/"):
            axis, _, item = part.partition("=")
            segs.append((axis, item))
        return cls(tuple(segs))
