from __future__ import annotations

from pathlib import Path
from typing import Any, Self
from dataclasses import dataclass, asdict, replace

from cascade.store.base import Store, StoreConfig


@dataclass
class FileConfig(StoreConfig):
    root: str | Path
    scope: tuple[str, ...] = ()

    def subscope(self, scope: tuple[str, ...]):
        return replace(self, scope=(*self.scope, *scope))

    def encode(self) -> dict[str, Any]:
        data = asdict(self)  # all fields are json compatible except scope
        data["scope"] = list(data["scope"])
        return data

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        raw = dict(raw)
        scope = tuple(raw.pop("scope", ()))
        return cls(scope=scope, **raw)


class FileStore(Store):
    """A local directory store. Scope is realised as nested path segments under
    ``root`` — but that's this backend's private choice, not the contract."""

    def __init__(
        self,
        config: FileConfig,
    ):
        self.config = config
        self.root = Path(config.root)
        self.scope = tuple(config.scope)
        self._build_path("").parent.mkdir(parents=True, exist_ok=True)

    def _build_path(self, key: str | None = None, at: tuple[str, ...] = ()) -> Path:
        parts = [*self.scope, *at]
        if key:
            parts.append(key)
        return self.root.joinpath(*parts)

    def put(
        self,
        key: str,
        data: bytes,
        at: tuple[str, ...] = (),
    ) -> str:
        p = self._build_path(key, at)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
        return key

    def get(
        self,
        key: str,
        at: tuple[str, ...] = (),
    ) -> bytes:
        return self._build_path(key, at).read_bytes()

    def has(self, key: str, at: tuple[str, ...] = ()) -> bool:
        return self._build_path(key, at).exists()

    def list(self, at: tuple[str, ...] = ()) -> list[str]:
        """List keys under a prefix (relative to the scoped root)."""
        base = self._build_path(at=at)
        if not base.exists():
            return []
        out = []
        for p in base.rglob("*"):
            if p.is_file():
                out.append(str(p.relative_to(base)))
        return out
