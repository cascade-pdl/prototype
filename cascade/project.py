"""The project file (``cascade.toml``): a project's *identity*, nothing more.

A project is nominal — a name and pointers to the two documents that do the real
work: the pipeline definition (*what* to run) and a default deployment (*what to
run it on*). Deliberately it carries **no** store, runner, or substrate config;
all of that lives in the deployment (see ``cascade.deployment``), because the same
project must be deployable against different substrates (a local file store in
dev, an S3 bucket in prod) without its identity changing.

This module owns one piece of ambient behaviour the deployment loader pointedly
does *not*: discovery. ``find_project_file`` walks up the directory tree for the
nearest ``cascade.toml`` — the "which project am I in?" question a CLI invoked
from a subdirectory needs answered. Loading is read-only (``tomllib`` is stdlib
and read-only); ``encode`` returns a TOML-serialisable dict for callers that want
to persist it via a writer of their choice.
"""
from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self


PROJECT_FILE = "cascade.toml"


class ProjectNotFound(Exception):
    """No ``cascade.toml`` was found walking up from the start directory."""


@dataclass
class Project:
    """The decoded ``cascade.toml``. ``pipeline`` and ``deployment`` are paths
    relative to the project root (the directory the file lives in)."""

    name: str
    pipeline: str = "pipeline.yaml"
    deployment: str = "deployment.yaml"

    def encode(self) -> dict[str, Any]:
        """A TOML-serialisable dict under a ``[project]`` table. (Kept as a dict
        rather than a string: ``tomllib`` is read-only, so writing is the
        caller's choice of writer — e.g. ``tomli_w.dump(project.encode(), f)``.)"""
        return {
            "project": {
                "name": self.name,
                "pipeline": self.pipeline,
                "deployment": self.deployment,
            }
        }

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> Self:
        # accept either a ``[project]`` table or a flat top-level mapping
        proj = raw.get("project", raw)
        return cls(
            name=proj["name"],
            pipeline=proj.get("pipeline", "pipeline.yaml"),
            deployment=proj.get("deployment", "deployment.yaml"),
        )

    @classmethod
    def load(cls, path: Path | str) -> Self:
        with open(path, "rb") as f:  # tomllib requires binary mode
            raw = tomllib.load(f)
        return cls.decode(raw)

    def pipeline_file(self, root: Path | str) -> Path:
        """Resolve the pipeline path against the project root."""
        return Path(root) / self.pipeline

    def deployment_file(self, root: Path | str) -> Path:
        """Resolve the default deployment path against the project root."""
        return Path(root) / self.deployment


def find_project_file(start: Path | str | None = None) -> Path:
    """Walk upward from ``start`` (default: cwd) to the nearest ``cascade.toml``.

    ``start`` may be a directory or a file (a file starts the search from its
    parent). Returns the path to the project file; raises ``ProjectNotFound`` if
    none exists up to the filesystem root.
    """
    origin = Path(start).resolve() if start is not None else Path.cwd()
    base = origin if origin.is_dir() else origin.parent
    for directory in (base, *base.parents):
        candidate = directory / PROJECT_FILE
        if candidate.is_file():
            return candidate
    raise ProjectNotFound(
        f"no {PROJECT_FILE} found in {base} or any parent directory"
    )


def find_project(start: Path | str | None = None) -> tuple[Project, Path]:
    """Discover, then load. Returns ``(project, root)`` where ``root`` is the
    directory containing the project file — the base for its relative paths."""
    path = find_project_file(start)
    return Project.load(path), path.parent