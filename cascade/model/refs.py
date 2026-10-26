from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from cascade.model.runner_kinds import RunnerKind
from cascade.model.ref_data import RefData, decode
from cascade.model.resources import Resources
from cascade.types import IoDecl


@dataclass
class Ref:
    """
    The representation of a YAML ref object

    note: at runtime, each runner will be instantiated merging the following:
    * the ref config (the docker image, the subprocess command)
    * the ref's resources (minimums; the task is sized to exactly these)
    * the backend the deployment places it on (ecs cluster, other params)

    ``runner`` is derived from ``config``: the YAML tag picks the RefData class, and the
    class is the kind from then on, so the two cannot disagree. ``requires`` names
    capabilities a backend must ``provide`` (``www``, ``private``); it never names a
    backend. ``limit`` names the class of work this ref belongs to (``heavy``,
    ``portal_api``); the deployment's ``limits:`` says how many of that class run at once.

    YAML::

        refs:
          - name: flat-bug
            runner: docker
            config:
              image: 123456789.dkr.ecr.eu-west-1.amazonaws.com/flat-bug:v3
            resources: { cpu: 4, memory: 32GiB, gpu: 1 }
            requires: [www]
            limit: heavy
            input:
              - { name: image, type: "io.Image" }
            output:
              - { name: detections, type: "ecology.Detection[]" }
    """

    name: str
    config: RefData
    resources: Resources | None = None
    requires: tuple[str, ...] = ()
    limit: str | None = None
    input: list[IoDecl] = field(default_factory=list)
    output: list[IoDecl] = field(default_factory=list)

    @property
    def runner(self) -> RunnerKind:
        return self.config.kind

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> "Ref":
        kind = RunnerKind(raw["runner"])
        if "overrides" in raw:
            # a silently dropped setting would run the work some other way than written
            raise ValueError(
                f"ref {raw.get('name')!r}: `overrides:` was removed; sizes go in "
                "`resources:`, machine settings in the deployment's backend config"
            )
        resources = raw.get("resources")
        limit = raw.get("limit")
        if limit is not None and (not isinstance(limit, str) or not limit):
            raise ValueError(f"ref {raw.get('name')!r}: `limit` must be a label, got {limit!r}")
        return cls(
            name=raw["name"],
            config=decode(kind, raw["config"]),
            resources=Resources.decode(resources) if resources is not None else None,
            requires=tuple(raw.get("requires") or ()),
            limit=limit,
            input=[IoDecl.decode(i) for i in raw.get("input", [])],
            output=[IoDecl.decode(o) for o in raw.get("output", [])],
        )