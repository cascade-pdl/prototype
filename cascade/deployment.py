"""The deployment file (``deployment.yaml``): *what a project runs on*.

The substrate half of the project/deployment split. Where ``cascade.toml`` names
the project, the deployment binds it to concrete infrastructure: the store backend
(a local directory vs an S3 bucket) and per-runner-kind defaults (cpu/memory,
pull policy) that the executor merges over each ref's own config at spawn time.

The **store lives here, base scope included** — it is a substrate concern, not an
identity one, so the same project can target different buckets/prefixes per
deployment. Scope is not a separate field: it is already a field of the concrete
``StoreConfig`` (``FileConfig``/``S3Config``), so it rides inside the store block.

Unlike the project loader, this module does **no discovery**: a deployment is
loaded from an explicit path (the caller — a CLI command, or the project's
``deployment`` pointer — decides which file). It holds a ``StoreConfig`` (inert
data), not a live ``Store``; materialising the backend (mkdir, boto client) is
the consumer's job at use time.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Self

import yaml

from cascade.store.base import StoreConfig
from cascade.store.registry import encode_config, decode_config
from cascade.model.runner_kinds import RunnerKind
from cascade.model.runner_overrides import RunnerOverrides, parse as parse_overrides


@dataclass
class Deployment:
    """The decoded ``deployment.yaml``.

    ``store`` is required (a deployment with nowhere to put data is meaningless).
    ``runners`` maps a runner kind to its deployment-level overrides — the
    defaults leg merged under each ref's own overrides by the executor.
    """

    store: StoreConfig
    name: str | None = None
    runners: dict[RunnerKind, RunnerOverrides] = field(default_factory=dict)

    def encode(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "store": encode_config(self.store),
            "runners": {
                kind.value: override.encode()
                for kind, override in self.runners.items()
            },
        }

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        runners: dict[RunnerKind, RunnerOverrides] = {}
        for key, value in (raw.get("runners") or {}).items():
            kind = RunnerKind(key)
            override = parse_overrides(kind, value)
            if override is not None:  # kinds without overrides parse to None
                runners[kind] = override
        return cls(
            store=decode_config(raw["store"]),
            name=raw.get("name"),
            runners=runners,
        )

    @classmethod
    def load(cls, path: Path | str) -> Self:
        """Load from an explicit path. No discovery — the caller chooses the file."""
        with open(path) as f:
            raw = yaml.safe_load(f)
        return cls.decode(raw)

    def dump(self) -> str:
        """Serialise back to YAML (pyyaml is already a dependency)."""
        return yaml.safe_dump(self.encode(), sort_keys=False)

    def save(self, path: Path | str) -> None:
        with open(path, "w") as f:
            f.write(self.dump())