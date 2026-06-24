from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from cascade.model.runner_overrides import RunnerOverrides, parse
from cascade.model.runner_kinds import RunnerKind
from cascade.model.ref_data import RefData, decode
from cascade.model.types import IoDecl


@dataclass
class Ref:
    """
    The representation of a YAML ref object

    note: at runtime, each runner will be instantiated merging the following:
    * the ref override (ref specific cpu/ram limits)
    * the ref config (the docker image, the subprocess command)
    * the declared deployment values (ecs cluster, other params)

    YAML::

        refs:
          - name: flat-bug
            runner: docker
            config:
              image: 123456789.dkr.ecr.eu-west-1.amazonaws.com/flat-bug:v3
            input:
              - { name: image, type: "io.Image" }
            output:
              - { name: detections, type: "ecology.Detection[]" }
    """

    name: str
    runner: RunnerKind
    config: RefData
    overrides: RunnerOverrides | None = None
    input: list[IoDecl] = field(default_factory=list)
    output: list[IoDecl] = field(default_factory=list)

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> "Ref":
        kind = RunnerKind(raw["runner"])
        return cls(
            name=raw["name"],
            runner=kind,
            config=decode(kind, raw["config"]),
            overrides=parse(kind, raw.get("overrides", None)),
            input=[IoDecl.decode(i) for i in raw.get("input", [])],
            output=[IoDecl.decode(o) for o in raw.get("output", [])],
        )

    def output_field(self, name: str | None) -> IoDecl | None:
        """Resolve an output field by name, or the sole output if name is None."""
        if name is None:
            return self.output[0] if len(self.output) == 1 else None
        return next((o for o in self.output if o.name == name), None)
