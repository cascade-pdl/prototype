"""Type annotations — refinements that narrow a primitive without changing it.

``str<s3-uri>`` is a ``string``: it goes wherever a ``string`` goes, carries a promise
about its *contents*, and says something a bare ``string`` cannot. That promise is worth
having because the alternative is a field called ``image`` whose type is ``string`` and
whose meaning lives in someone's head.

**Deliberately not a hierarchy.** The compatibility rule is *identical or omitted*:
``str<s3-uri>`` satisfies ``string``, and ``string`` does not satisfy ``str<s3-uri>``.
No transitive closure, no wildcards, no interaction with structural ``extends`` — one
clause in one comparison. Refinement-type systems get expensive precisely where they
grow rules, so this one is fixed at the cheapest rule that is still sound: you may
forget a promise, you may not invent one.

Annotations qualify **primitives only**. A structure's shape is already its type, so
``Crop<something>`` would have nothing to mean.

Validation of *values* belongs in the node-side SDK, which is the only place values
exist — the compiler has types and no data. ``pattern`` is carried here so the SDK has
one place to look, and so that the set of legal annotations is checkable at compile
time: an unregistered ``str<typoo>`` should be an error, not a promise nobody keeps.
"""
from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class Annotation:
    """One registered annotation."""

    name: str
    bases: frozenset[str]
    pattern: str | None = None
    description: str = ""

    def matches(self, value: str) -> bool:
        """Whether a value honours the promise. Used by the SDK, not the compiler."""
        if self.pattern is None:
            return True
        return re.fullmatch(self.pattern, value) is not None


ANNOTATIONS: dict[str, Annotation] = {
    a.name: a
    for a in (
        Annotation(
            name="uri",
            bases=frozenset({"string"}),
            pattern=r"[a-zA-Z][a-zA-Z0-9+.\-]*:.+",
            description="an absolute URI with a scheme",
        ),
        Annotation(
            name="s3-uri",
            bases=frozenset({"string"}),
            pattern=r"s3://[^/]+/.*",
            description="an s3://bucket/key reference",
        ),
        Annotation(
            name="path",
            bases=frozenset({"string"}),
            pattern=None,
            description="a filesystem path; deliberately unvalidated, since what is "
            "legal depends on the platform the node runs on",
        ),
        Annotation(
            name="email",
            bases=frozenset({"string"}),
            pattern=r"[^@\s]+@[^@\s]+\.[^@\s]+",
            description="an email address",
        ),
    )
}


def is_registered(name: str) -> bool:
    return name in ANNOTATIONS


def get(name: str) -> Annotation | None:
    return ANNOTATIONS.get(name)


def known() -> list[str]:
    return sorted(ANNOTATIONS)


def check_annotation(base: str, annotation: str | None) -> str | None:
    """Whether ``annotation`` is legal on ``base``; a reason if not, else ``None``.

    This is type-system knowledge, so it lives here rather than in ``plan.validate``:
    the compiler knows which types *this pipeline* declared, the type system knows what
    a type *means*. Note it is a spelling check only — whether two annotated types are
    compatible is ``TypeExpr.accepts``, which never consults this registry.
    """
    if annotation is None:
        return None
    known_annotation = get(annotation)
    if known_annotation is None:
        return f"unknown annotation {annotation!r} (known: {', '.join(known())})"
    if base not in known_annotation.bases:
        # a structure's shape is already its type, so there is nothing for an
        # annotation on one to mean
        return (
            f"annotation {annotation!r} does not apply to {base!r} "
            f"(applies to: {', '.join(sorted(known_annotation.bases))})"
        )
    return None