"""``TypeExpr`` — a type expression, and the one rule for comparing two of them.

Lives beside the structures and annotations it refers to rather than in ``plan``: a
type expression is not a plan concept, it is what a plan *persists*. The node-side SDK
needs it without dragging in the compiler, and structure fields use it as much as ports
do.

The comparison rule is deliberately small. Base and depth must match exactly; the
annotation may be *forgotten but never invented*. Refinement-type systems get expensive
where they grow rules, so this one is fixed at the cheapest rule that is still sound.
"""
from __future__ import annotations

from dataclasses import dataclass


class TypeError_(Exception):
    """Problem with a type expression."""


@dataclass(frozen=True)
class TypeExpr:
    """A type expression: a base type, an array nesting depth, and an optional
    annotation refining the base.

    ``"Detection[]"`` -> base="Detection", depth=1; ``"float"`` -> depth=0;
    ``"string<s3-uri>[]"`` -> base="string", depth=1, annotation="s3-uri".
    Serializes as its rendered string form.

    An annotation narrows without changing the type: it is carried, rendered and
    round-tripped, but the *base* is untouched, so every existing lookup
    (``is_defined``, ``is_subtype``) keeps working on the base alone.
    """

    base: str
    depth: int
    annotation: str | None = None

    @classmethod
    def parse(cls, s: str) -> "TypeExpr":
        """Parse a declared type, rejecting anything malformed.

        Strict on purpose, and called at *decode* rather than at each use: an
        unparseable expression should be reported against the document that declared
        it, not surface later as a confusing "unknown type 'foo<bar'". Whether the base
        *exists* is a separate question — that needs the pipeline's ``TypeEnv``, so it
        stays with the compiler.
        """
        original = s
        s = s.strip()
        depth = 0
        while s.endswith("[]"):
            s, depth = s[:-2], depth + 1

        annotation = None
        if "<" in s or ">" in s:
            if s.count("<") != 1 or not s.endswith(">"):
                raise TypeError_(
                    f"malformed type {original!r}: an annotation is written "
                    "'base<name>', with one '<' and a closing '>'"
                )
            s, _, annotation = s[:-1].partition("<")
            annotation = annotation.strip()
            if not annotation:
                raise TypeError_(f"malformed type {original!r}: empty annotation")
            if ">" in annotation or "[" in annotation or "]" in annotation:
                raise TypeError_(
                    f"malformed type {original!r}: annotation {annotation!r} contains "
                    "a reserved character"
                )

        base = s.strip()
        if not base:
            raise TypeError_(f"malformed type {original!r}: no base type")
        if any(c in base for c in "[]<> "):
            raise TypeError_(
                f"malformed type {original!r}: base {base!r} contains a reserved "
                "character; array brackets belong at the end"
            )
        return cls(base, depth, annotation)

    def render(self) -> str:
        suffix = f"<{self.annotation}>" if self.annotation else ""
        return self.base + suffix + "[]" * self.depth

    def accepts(self, other: "TypeExpr") -> bool:
        """Whether a value of ``other`` may be supplied where ``self`` is expected.

        Base and depth must match exactly. The annotation rule is *identical or
        omitted*: a promise may be forgotten, never invented. So ``string`` accepts
        ``string<s3-uri>``, and ``string<s3-uri>`` does not accept ``string``.
        """
        if self.base != other.base or self.depth != other.depth:
            return False
        return self.annotation is None or self.annotation == other.annotation

    def as_collection(self) -> "TypeExpr":
        return TypeExpr(self.base, self.depth + 1, self.annotation)

    def element(self) -> "TypeExpr":
        """One element of this collection; the annotation rides along, since it
        qualifies the base rather than the nesting."""
        if self.depth < 1:
            raise TypeError_(f"cannot take element of non-collection {self.render()!r}")
        return TypeExpr(self.base, self.depth - 1, self.annotation)

    # serialization is just the string form
    def encode(self) -> str:
        return self.render()

    @classmethod
    def decode(cls, raw: str) -> "TypeExpr":
        return cls.parse(raw)