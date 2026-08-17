"""The type system: expressions, annotations, and the structures they name.

Split by concern but re-exported flat, so ``from cascade.model.types import X`` reaches
any of them and existing imports are undisturbed:

- ``expr`` — ``TypeExpr``: parsing, rendering, and the compatibility rule.
- ``annotations`` — the annotation registry and its legality check.
- ``structures`` — ``Structure``, ``FieldDecl``, ``IoDecl``, ``DataFormat``.

Kept as ``model/types/`` rather than a top-level package: ``cascade/types/`` or
``cascade/typing/`` would shadow stdlib module names, which resolves correctly under
absolute imports but reads as a trap.
"""
from cascade.model.types.annotations import (
    ANNOTATIONS,
    Annotation,
    check_annotation,
    get,
    is_registered,
    known,
)
from cascade.model.types.expr import TypeError_, TypeExpr
from cascade.model.types.structures import (
    DataFormat,
    FieldDecl,
    IoConfig,
    IoDecl,
    IOField,
    Structure,
    TypesSection,
)

__all__ = [
    "ANNOTATIONS",
    "Annotation",
    "check_annotation",
    "get",
    "is_registered",
    "known",
    "TypeError_",
    "TypeExpr",
    "DataFormat",
    "FieldDecl",
    "IoConfig",
    "IoDecl",
    "IOField",
    "Structure",
    "TypesSection",
]