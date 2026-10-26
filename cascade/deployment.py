"""The deployment file (``deployment.yaml``): *what a project runs on*.

The substrate half of the project/deployment split. Where ``cascade.toml`` names
the project, the deployment binds it to concrete infrastructure: the store backend
(a local directory vs an S3 bucket) and an ordered list of *backends* — the places refs
can run, each with the largest task it accepts and what it provides. Refs
state what they need; placement matches them to the first backend that fits.

``limits:`` caps classes of work wherever they run: ``{heavy: 3, portal_api: 5}``. A ref
names its class (``limit: heavy``), the deployment says how many run at once. The label
describes the work, so it is the ref's; the number depends on what the work runs against,
so it is the deployment's. A backend's own cap is its ``concurrency``.

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
from cascade.store.file_store import FileConfig
from cascade.store.registry import encode_config, decode_config
from cascade.substrate.backends import Backend, BackendError, BackendKind


class DeploymentError(ValueError):
    """A deployment file that does not describe a usable environment."""


@dataclass
class Deployment:
    """The decoded ``deployment.yaml``.

    ``store`` is required (a deployment with nowhere to put data is meaningless).
    ``backends`` is ordered: placement takes the first that fits, so order is priority.
    ``limits`` maps a ref's ``limit:`` label to how many such refs run at once.
    """

    store: StoreConfig
    name: str | None = None
    backends: list[Backend] = field(default_factory=list)
    limits: dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        names = [b.name for b in self.backends]
        duplicates = sorted({n for n in names if names.count(n) > 1})
        if duplicates:
            raise DeploymentError(f"backend name(s) used twice: {duplicates}")

    def backend(self, name: str) -> Backend | None:
        return next((b for b in self.backends if b.name == name), None)

    def check(self) -> list[str]:
        """What is wrong with this deployment as a whole, as opposed to one entry
        (entries are checked as they decode)."""
        errors = []
        if isinstance(self.store, FileConfig):
            for b in self.backends:
                if b.kind is BackendKind.ecs:
                    errors.append(
                        f"backend {b.name!r} runs on ECS, which cannot see a file store; "
                        "use an s3 store"
                    )
        return errors

    def encode(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "store": encode_config(self.store),
            "backends": [b.encode() for b in self.backends],
            "limits": dict(self.limits),
        }

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        if "runners" in raw:
            # a silently ignored block would run the work with settings nobody chose
            raise DeploymentError(
                "`runners:` was replaced by `backends:`: local docker settings "
                "go in a `kind: local` backend's config, sizes in each ref's `resources:`"
            )
        unknown = set(raw) - {"name", "store", "backends", "limits"}
        if unknown:
            raise DeploymentError(f"unknown deployment field(s): {sorted(unknown)}")
        try:
            backends = [Backend.decode(b) for b in raw.get("backends") or ()]
        except BackendError as e:
            raise DeploymentError(str(e))
        return cls(
            store=decode_config(raw["store"]),
            name=raw.get("name"),
            backends=backends,
            limits=_decode_limits(raw.get("limits") or {}),
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


def _decode_limits(raw: Any) -> dict[str, int]:
    if not isinstance(raw, dict):
        raise DeploymentError(f"`limits` maps a label to a number of tasks, got {raw!r}")
    bad = {
        label: n for label, n in raw.items()
        if isinstance(n, bool) or not isinstance(n, int) or n < 1
    }
    if bad:
        raise DeploymentError(f"limits must be positive integers: {bad}")
    return {str(label): n for label, n in raw.items()}