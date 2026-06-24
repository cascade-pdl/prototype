from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Self


@dataclass
class FieldDecl:
    """One field of a structure: a name and a type expression string."""

    name: str
    type: str  # raw type expression, e.g. "float", "string<uuid>", "Detection[]"

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        return cls(name=raw["name"], type=raw["type"])

    def encode(self) -> dict[str, Any]:
        return {"name": self.name, "type": self.type}


@dataclass
class Structure:
    """A named record type. ``extends`` gives single-inheritance (structural
    subtyping); the child has all the parent's fields plus its own."""

    name: str
    fields: list[FieldDecl] = field(default_factory=list)
    extends: str | None = None

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        return cls(
            name=raw["name"],
            fields=[FieldDecl.decode(f) for f in raw.get("fields", [])],
            extends=raw.get("extends"),
        )

    def encode(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "fields": [f.encode() for f in self.fields],
            "extends": self.extends,
        }


@dataclass
class TypesSection:
    structures: list[Structure] = field(default_factory=list)

    @classmethod
    def decode(cls, raw: dict[str, Any] | list[dict[str, Any]]) -> Self:
        items = raw.get("structures", []) if isinstance(raw, dict) else raw
        return cls(structures=[Structure.decode(s) for s in items])

    def encode(self) -> dict[str, Any]:
        return {"structures": [s.encode() for s in self.structures]}


class DataFormat(str, Enum):
    csv = "csv"
    json = "json"


@dataclass
class IOField:
    name: str
    type: str


@dataclass
class IoConfig:
    encoding: DataFormat = DataFormat.json
    mapping: dict[str, str] = field(default_factory=dict)

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        return cls(
            encoding=DataFormat(raw.get("encoding", DataFormat.json.value)),
            mapping=dict(raw.get("mapping", {})),
        )

    def encode(self) -> dict[str, Any]:
        return {"encoding": self.encoding.value, "mapping": dict(self.mapping)}


@dataclass
class IoDecl(IOField):
    """One named input or output port with a type expression and IO config."""

    config: IoConfig = field(default_factory=IoConfig)

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        return cls(
            name=raw["name"],
            type=raw["type"],
            config=IoConfig.decode(raw.get("config", {})),
        )

    def encode(self) -> dict[str, Any]:
        return {"name": self.name, "type": self.type, "config": self.config.encode()}
