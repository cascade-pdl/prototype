"""The deployment file (``deployment.yaml``): *what a project runs on*.

The substrate half of the project/deployment split. Where ``cascade.toml`` names
the project, the deployment binds it to concrete infrastructure: the store backend
(a local directory vs an S3 bucket) and an ordered list of *backends* — the places refs
can run, each with the largest task it accepts and what it provides. Refs
state what they need; placement matches them to the first backend that fits.

A backend's own cap is its ``concurrency``: a fact about that machine or cluster. Named
limits (how many ``bulky`` refs at once) are not here: they are about the work, so the
pipeline declares them.

The **store lives here** — a substrate concern, so the same project can target
different buckets/prefixes per deployment. Where in it a project's runs land starts
with the project's own name (its identity, from ``cascade.toml``), added by whoever
holds the project when it resolves the store; the deployment adds its own ``scope``
after it, or turns the project segment off with ``project_scope: false``. Both sit in
the store block beside ``kind``, not inside ``config``: scope is not a backend setting
(every store has one), and ``config`` holds only what a backend needs. Decoded, the
scope lands in the ``StoreConfig``, which is what nodes receive; ``config.scope`` in a
file is refused, so it is written in one place.

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
    """

    store: StoreConfig
    name: str | None = None
    backends: list[Backend] = field(default_factory=list)
    project_scope: bool = True
    """Whether a project's runs land under the project's name first (the default). The
    deployment's own ``store.scope`` is already in ``store``; the name is added by the
    caller holding the project (``cascade.cli._resolve.build_store``)."""

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
        block = encode_config(self.store)
        config = dict(block["config"])
        scope = config.pop("scope", [])
        return {
            "name": self.name,
            "store": {
                "kind": block["kind"],
                "scope": list(scope),
                "project_scope": self.project_scope,
                "config": config,
            },
            "backends": [b.encode() for b in self.backends],
        }

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        if "runners" in raw:
            # a silently ignored block would run the work with settings nobody chose
            raise DeploymentError(
                "`runners:` was replaced by `backends:`: local docker settings "
                "go in a `kind: local` backend's config, sizes in each ref's `resources:`"
            )
        if "limits" in raw:
            # ignored, the work would run uncapped
            raise DeploymentError(
                "`limits:` moved to the pipeline: declare it at the top of pipeline.yaml, "
                "beside `refs:`; a deployment caps backends only, with `concurrency`"
            )
        unknown = set(raw) - {"name", "store", "backends"}
        if unknown:
            raise DeploymentError(f"unknown deployment field(s): {sorted(unknown)}")
        try:
            backends = [Backend.decode(b) for b in raw.get("backends") or ()]
        except BackendError as e:
            raise DeploymentError(str(e))
        store, project_scope = _decode_store(raw.get("store"))
        return cls(
            store=store,
            name=raw.get("name"),
            backends=backends,
            project_scope=project_scope,
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


def _decode_store(raw: Any) -> tuple[StoreConfig, bool]:
    """A ``store:`` block: ``kind``, ``config``, and beside them ``scope`` and
    ``project_scope``. The scope goes into the ``StoreConfig``."""
    if not isinstance(raw, dict) or "kind" not in raw:
        raise DeploymentError("`store:` needs a `kind` (file or s3) and its `config`")
    unknown = set(raw) - {"kind", "config", "scope", "project_scope"}
    if unknown:
        raise DeploymentError(f"unknown store field(s): {sorted(unknown)}")
    config = dict(raw.get("config") or {})
    if "scope" in config:
        raise DeploymentError(
            "`store.config.scope` moved up: write `scope: [...]` in the store block, beside "
            "`kind`; the project's name is added in front of it unless `project_scope: false`"
        )
    scope = raw.get("scope") or []
    if not isinstance(scope, list) or not all(isinstance(s, str) and s for s in scope):
        raise DeploymentError(f"`store.scope` must be a list of names, got {scope!r}")
    project_scope = raw.get("project_scope", True)
    if not isinstance(project_scope, bool):
        raise DeploymentError(f"`store.project_scope` must be true or false, got {project_scope!r}")
    try:
        store = decode_config({"kind": raw["kind"], "config": {**config, "scope": scope}})
    except (TypeError, ValueError) as e:
        raise DeploymentError(f"invalid store: {e}")
    return store, project_scope
