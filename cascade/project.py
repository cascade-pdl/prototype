"""Project configuration: ``cascade.toml``.

A cascade project is a YAML + Docker project — NOT a Python project (cascade's
own implementation language is irrelevant to the user). So project identity and
metadata live in a language-agnostic ``cascade.toml`` at the project root,
distinct from the *deployment* file (which holds environment/cluster/bucket
wiring) and from per-run CLI args. The three tiers:

    cascade.toml   — true regardless of environment or run (name, version, ...)
    deployment.yaml — this environment (registry, cluster, bucket, roles)
    CLI args        — this run (inputs, run-id, tag)

Schema (``[cascade-project]``):
    name           required; the project name. Load-bearing: it drives the
                   naming convention (ECR repos, taskdef families), so it must
                   be registry-safe (lowercase, digits, hyphens).
    version        project version (provenance; distinct from image tags)
    maintainers    list of contacts
    description    free text
    pipeline_file  default pipeline path (so commands needn't be given one)
    include_types  optional list of shared type files, merged into every
                   pipeline's types at load (project-wide reusable types)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

try:
    import tomllib  # py3.11+
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore


# registry-safe: lowercase letters, digits, hyphens; must start alphanumeric.
# (ECR repo names and ECS taskdef families derive from this, so constrain it.)
_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")


class ProjectError(Exception):
    pass


@dataclass
class ProjectConfig:
    name: str
    scope: str
    version: str = "0.0.1"
    maintainers: list[str] = field(default_factory=list)
    description: str = ""
    pipeline_file: str = "pipeline.yaml"
    include_types: list[str] = field(default_factory=list)
    # where this config was loaded from (so include_types/pipeline_file resolve
    # relative to the project root, regardless of cwd)
    root: Path = field(default=Path("."))

    @staticmethod
    def validate_name(name: str) -> None:
        if not _NAME_RE.match(name):
            raise ProjectError(
                f"project name '{name}' is not valid: use lowercase letters, "
                f"digits and hyphens, starting with a letter or digit "
                f"(it becomes part of ECR repo / ECS taskdef names)"
            )

    @classmethod
    def load(cls, path: str | Path = "cascade.toml") -> "ProjectConfig":
        p = Path(path)
        if not p.exists():
            raise ProjectError(
                f"no project config found at '{p}': cascade projects need a "
                f"cascade.toml (run `cascade authoring new` to scaffold one)"
            )
        with open(p, "rb") as f:
            raw = tomllib.load(f)
        proj = raw.get("cascade-project")
        if proj is None:
            raise ProjectError(f"{p}: missing [cascade-project] section")
        if "name" not in proj:
            raise ProjectError(f"{p}: [cascade-project] requires a 'name'")
        cls.validate_name(proj["name"])
        # scope is REQUIRED and non-nullable: it sub-scopes this project's data
        # in the (shared) store, independent of the store backend. No project may
        # be un-partitioned, so absence is a hard error.
        if not proj.get("scope"):
            raise ProjectError(
                f"{p}: [cascade-project] requires a 'scope' (the store sub-scope "
                f"for this project's data, e.g. scope = \"{proj['name']}\")")
        cls.validate_name(proj["scope"])   # same registry-safe rules
        return cls(
            name=proj["name"],
            scope=proj["scope"],
            version=str(proj.get("version", "0.0.1")),
            maintainers=list(proj.get("maintainers") or []),
            description=proj.get("description", ""),
            pipeline_file=proj.get("pipeline_file", "pipeline.yaml"),
            include_types=list(proj.get("include_types") or []),
            root=p.resolve().parent,
        )

    def resolve(self, rel: str) -> Path:
        """Resolve a path (pipeline_file, include_types entry) relative to the
        project root."""
        return self.root / rel
