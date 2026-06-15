"""The Cascade type system.

This is the differentiated idea, so it is implemented fully (not stubbed).

A *type expression* is the string in a contract: ``int``, ``float``,
``string<uuid>``, ``ecology.Detection``, ``Detection[]``, ``MothCrop?``,
``float[][]``. It parses into a :class:`TypeExpr` with a base, an optional
``<format>``, a number of trailing ``[]`` (array dimensions), and a trailing
``?`` (nullable).

The :class:`TypeRegistry` holds named structures and answers subtyping
questions. :func:`check_edge` is the load-time check that an upstream output,
after the edge's transform (scatter unwraps an array, gather wraps one), is
accepted by a downstream input.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from .model import Structure, TypesSection


PRIMITIVES = {"string", "int", "float", "bool"}


class TypeError_(Exception):
    """A type-system error (named with a trailing underscore to avoid shadowing
    the builtin)."""


# --------------------------------------------------------------------------- #
# TypeExpr — parsed type expression
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TypeExpr:
    base: str                 # primitive ("int") or named ("ecology.Detection")
    format: str | None = None # the <...> modifier on a primitive
    array_dims: int = 0       # number of trailing []
    nullable: bool = False    # trailing ?

    @property
    def is_primitive(self) -> bool:
        return self.base in PRIMITIVES

    @property
    def is_array(self) -> bool:
        return self.array_dims > 0

    def element(self) -> "TypeExpr":
        """The element type when one [] is removed (scatter)."""
        if self.array_dims == 0:
            raise TypeError_(f"cannot take element of non-array type '{self}'")
        return TypeExpr(self.base, self.format, self.array_dims - 1, self.nullable)

    def arrayed(self) -> "TypeExpr":
        """This type wrapped in one more [] (gather)."""
        return TypeExpr(self.base, self.format, self.array_dims + 1, False)

    def __str__(self) -> str:
        s = self.base
        if self.format:
            s += f"<{self.format}>"
        s += "[]" * self.array_dims
        if self.nullable:
            s += "?"
        return s


_TYPE_RE = re.compile(
    r"""^
    (?P<base>[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)
    (?:<(?P<format>[A-Za-z0-9_]+)>)?
    (?P<arrays>(?:\[\])*)
    (?P<nullable>\?)?
    $""",
    re.VERBOSE,
)


def parse_type(s: str) -> TypeExpr:
    s = s.strip()
    if not s:
        raise TypeError_("empty type expression")
    m = _TYPE_RE.match(s)
    if not m:
        raise TypeError_(f"malformed type expression: '{s}'")
    base = m.group("base")
    fmt = m.group("format")
    dims = len(m.group("arrays")) // 2
    nullable = m.group("nullable") is not None
    if fmt and base not in PRIMITIVES:
        raise TypeError_(f"format <{fmt}> is only valid on primitives, not '{base}'")
    return TypeExpr(base=base, format=fmt, array_dims=dims, nullable=nullable)


# --------------------------------------------------------------------------- #
# TypeRegistry — named structures + subtyping
# --------------------------------------------------------------------------- #
@dataclass
class TypeRegistry:
    structures: dict[str, Structure] = field(default_factory=dict)

    @classmethod
    def from_section(cls, section: TypesSection) -> "TypeRegistry":
        reg = cls()
        for s in section.structures:
            reg.structures[s.name] = s
        # validate extends targets exist and there are no cycles
        for name in reg.structures:
            reg._flatten_fields(name, seen=set())
        return reg

    def _flatten_fields(self, name: str, seen: set[str]) -> dict[str, str]:
        """All fields (name -> type str) including inherited, checking cycles."""
        if name in seen:
            raise TypeError_(f"circular inheritance involving '{name}'")
        struct = self.structures.get(name)
        if struct is None:
            raise TypeError_(f"unknown structure '{name}'")
        fields: dict[str, str] = {}
        if struct.extends:
            fields.update(self._flatten_fields(struct.extends, seen | {name}))
        for f in struct.fields:
            fields[f.name] = f.type
        return fields

    def is_subtype_named(self, sub: str, sup: str) -> bool:
        """Is named structure ``sub`` a subtype of ``sup``? True if equal or if
        ``sub`` transitively extends ``sup`` (nominal-via-extends), OR if
        ``sub`` structurally has all of ``sup``'s fields with compatible types
        (structural subtyping)."""
        if sub == sup:
            return True
        # nominal: walk the extends chain
        cur = self.structures.get(sub)
        seen = set()
        while cur and cur.extends and cur.extends not in seen:
            seen.add(cur.extends)
            if cur.extends == sup:
                return True
            cur = self.structures.get(cur.extends)
        # structural: sub has every field of sup, compatibly
        if sub in self.structures and sup in self.structures:
            sub_fields = self._flatten_fields(sub, set())
            sup_fields = self._flatten_fields(sup, set())
            for fname, ftype in sup_fields.items():
                if fname not in sub_fields:
                    return False
                if not self.satisfies(parse_type(sub_fields[fname]), parse_type(ftype)):
                    return False
            return True
        return False

    def satisfies(self, sub: TypeExpr, sup: TypeExpr) -> bool:
        """Does a value of type ``sub`` satisfy an input expecting ``sup``?"""
        # array dimensions must match
        if sub.array_dims != sup.array_dims:
            return False
        # nullability: a non-nullable satisfies a nullable, not the reverse
        if sub.nullable and not sup.nullable:
            return False
        # primitives
        if sub.is_primitive or sup.is_primitive:
            if sub.base != sup.base:
                return False
            # a formatted primitive (string<uuid>) satisfies the bare primitive
            # (string); the reverse is not guaranteed
            if sup.format is not None and sub.format != sup.format:
                return False
            return True
        # named types: structural / nominal subtyping
        return self.is_subtype_named(sub.base, sup.base)


# --------------------------------------------------------------------------- #
# Edge transforms
# --------------------------------------------------------------------------- #
@dataclass
class EdgeWarning:
    message: str


def transform_for(mode: str, downstream_scatters: bool) -> str:
    """Which transform an edge applies. ``gather`` wraps T -> T[]; a scatter on
    the downstream node unwraps T[] -> T; otherwise pass-through."""
    if mode == "gather":
        return "gather"
    if downstream_scatters:
        return "scatter"
    return "single"


def apply_transform(transform: str, upstream: TypeExpr) -> TypeExpr:
    if transform == "single":
        return upstream
    if transform == "gather":
        return upstream.arrayed()
    if transform == "scatter":
        if not upstream.is_array:
            raise TypeError_(f"cannot scatter over non-array type '{upstream}'")
        return upstream.element()
    raise TypeError_(f"unknown transform '{transform}'")


def check_edge(
    registry: TypeRegistry,
    upstream_type: str,
    transform: str,
    downstream_type: str,
) -> list[EdgeWarning]:
    """Check one edge end to end. Raises :class:`TypeError_` on a hard mismatch,
    returns a list of non-fatal warnings (e.g. nullable narrowing)."""
    up = parse_type(upstream_type)
    down = parse_type(downstream_type)
    delivered = apply_transform(transform, up)
    warnings: list[EdgeWarning] = []
    if delivered.nullable and not down.nullable:
        warnings.append(
            EdgeWarning(
                f"upstream is nullable ('{delivered}') but downstream input is not "
                f"('{down}') — a null may reach a non-nullable input; add a filter "
                f"or mark the input nullable"
            )
        )
        # not fatal on its own; treat narrowing as a warning
        delivered = TypeExpr(delivered.base, delivered.format, delivered.array_dims, nullable=False)
    if not registry.satisfies(delivered, down):
        raise TypeError_(
            f"type mismatch: upstream provides '{delivered}' "
            f"(after {transform}) but downstream expects '{down}'"
        )
    return warnings
