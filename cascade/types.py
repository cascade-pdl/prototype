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
    base: str                 # primitive ("int"), named ("Detection"), or "binary"
    format: str | None = None # the <...> modifier on a primitive
    array_dims: int = 0       # number of trailing []
    nullable: bool = False    # trailing ?
    media_type: str | None = None  # for binary blobs: "image/png", "image/*", or None (=*/*)

    @property
    def is_primitive(self) -> bool:
        return self.base in PRIMITIVES

    @property
    def is_binary(self) -> bool:
        """An opaque blob: bytes + a media type. No fields, no codec, no rename."""
        return self.base == "binary"

    @property
    def is_array(self) -> bool:
        return self.array_dims > 0

    def element(self) -> "TypeExpr":
        """The element type when one [] is removed (scatter)."""
        if self.array_dims == 0:
            raise TypeError_(f"cannot take element of non-array type '{self}'")
        return TypeExpr(self.base, self.format, self.array_dims - 1, self.nullable, self.media_type)

    def arrayed(self) -> "TypeExpr":
        """This type wrapped in one more [] (gather)."""
        return TypeExpr(self.base, self.format, self.array_dims + 1, False, self.media_type)

    def __str__(self) -> str:
        s = self.base
        if self.is_binary and self.media_type:
            s += f"<{self.media_type}>"
        elif self.format:
            s += f"<{self.format}>"
        s += "[]" * self.array_dims
        if self.nullable:
            s += "?"
        return s


_TYPE_RE = re.compile(
    r"""^
    (?P<base>[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)
    (?:<(?P<qualifier>[^>]+)>)?
    (?P<arrays>(?:\[\])*)
    (?P<nullable>\?)?
    $""",
    re.VERBOSE,
)

# a media type is "type/subtype" where subtype may be "*"; type may not be "*"
# unless the whole thing is "*/*". (image/png, image/*, */* are valid; */png not.)
_MEDIA_RE = re.compile(r"^(?:\*/\*|[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]*/(?:\*|[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]*))$")


def parse_type(s: str) -> TypeExpr:
    s = s.strip()
    if not s:
        raise TypeError_("empty type expression")
    m = _TYPE_RE.match(s)
    if not m:
        raise TypeError_(f"malformed type expression: '{s}'")
    base = m.group("base")
    qualifier = m.group("qualifier")
    dims = len(m.group("arrays")) // 2
    nullable = m.group("nullable") is not None

    if base == "binary":
        # binary               -> any bytes (media_type None, i.e. */*)
        # binary<image/png>    -> exact media type
        # binary<image/*>      -> wildcard subtype
        media = None
        if qualifier is not None:
            if not _MEDIA_RE.match(qualifier):
                raise TypeError_(
                    f"invalid media type '{qualifier}' in binary<{qualifier}> "
                    f"(expected 'type/subtype', 'type/*', or '*/*'; '*/subtype' is not allowed)"
                )
            media = qualifier
        return TypeExpr(base="binary", format=None, array_dims=dims,
                        nullable=nullable, media_type=media)

    # non-binary: a <qualifier> is a primitive format and may not contain '/'
    if qualifier is not None:
        if base not in PRIMITIVES:
            raise TypeError_(f"format <{qualifier}> is only valid on primitives, not '{base}'")
        if "/" in qualifier:
            raise TypeError_(f"'/' is only valid in a binary media type, not in <{qualifier}>")
    return TypeExpr(base=base, format=qualifier, array_dims=dims, nullable=nullable)


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
        """Does a value of type ``sub`` (the producer/output) satisfy an input
        expecting ``sup`` (the consumer)?"""
        # array dimensions must match
        if sub.array_dims != sup.array_dims:
            return False
        # nullability: a non-nullable satisfies a nullable, not the reverse
        if sub.nullable and not sup.nullable:
            return False
        # binary blobs: media-type compatibility (directional, see _media_satisfies).
        # binary and non-binary are never compatible with each other.
        if sub.is_binary or sup.is_binary:
            if not (sub.is_binary and sup.is_binary):
                return False
            return _media_satisfies(sub.media_type, sup.media_type)
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


def _media_satisfies(out: str | None, inp: str | None) -> bool:
    """Directional media-type compatibility for binary blobs.

    ``out`` is the producer's media type, ``inp`` the consumer's. A specific
    output satisfies a wider (wildcard) input, but a wide output does NOT
    satisfy a more specific input (that would be unsafe narrowing).

    None means '*/*' (any). Examples:
      out=image/png  inp=image/*   -> True   (a PNG is an image)
      out=image/png  inp=image/png -> True   (exact)
      out=image/*    inp=image/png -> False  (producer might emit non-PNG)
      out=image/png  inp=audio/*   -> False  (different type)
      out=image/png  inp=None(*/*) -> True   (consumer accepts anything)
      out=None(*/*)  inp=image/png -> False  (producer could be anything)
    """
    out = out or "*/*"
    inp = inp or "*/*"
    o_type, o_sub = out.split("/")
    i_type, i_sub = inp.split("/")
    # consumer type must accept producer type
    if i_type != "*" and i_type != o_type:
        return False
    # if consumer accepts any type (*), it accepts this one
    if i_type == "*":
        # only an any-type input accepts an any-type output; otherwise fine
        return True
    # types match; check subtype
    if i_sub == "*":
        return True              # consumer accepts any subtype of this type
    if o_sub == "*":
        return False             # producer is unspecific, consumer wants exact -> unsafe
    return o_sub == i_sub        # both specific: must match exactly


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
