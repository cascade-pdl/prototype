"""The Plan: the authoring -> execution interface.

Everything the executor needs and nothing it does not — per-dag node graphs to
schedule, signatures to load/store data, per-ref run config to launch nodes, the
type environment for runtime payload validation, and the entrypoint. Pointedly
*not* the Pipeline, the passes, or graphlib.

It is strict and canonical: a given pipeline compiles to one Plan, every runnable
named anywhere has a signature, every ref has a run config, every dag has a node
graph. It must round-trip through JSON (decode(encode(plan)) == plan) so it can be
shipped to an executor in another process/container without pickling. ``version``
is checked on decode so a stale .plan fails loudly rather than misparses.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from cascade.graph import Graph
from cascade.model.dag_node import DagNode
from cascade.model.dependency import Dependency
from cascade.plan.signature import Signature
from cascade.plan.type_env import TypeEnv
from cascade.plan.run_config import RunConfig


PLAN_VERSION = 1


class PlanVersionError(Exception):
    """A .plan was produced by an incompatible compiler version."""


@dataclass
class Plan:
    entrypoint: str
    node_graphs: dict[str, Graph[DagNode, Dependency]]
    signatures: dict[str, Signature]
    run_config: dict[str, RunConfig]
    type_env: TypeEnv
    version: int = PLAN_VERSION

    def encode(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "entrypoint": self.entrypoint,
            "node_graphs": {
                name: g.encode(lambda n: n.encode(), lambda e: e.encode())
                for name, g in self.node_graphs.items()
            },
            "signatures": {name: s.encode() for name, s in self.signatures.items()},
            "run_config": {name: c.encode() for name, c in self.run_config.items()},
            "type_env": self.type_env.encode(),
        }

    @classmethod
    def decode(cls, raw: dict[str, Any]) -> "Plan":
        version = raw.get("version")
        if version != PLAN_VERSION:
            raise PlanVersionError(
                f"plan version {version!r} != supported {PLAN_VERSION!r}; recompile"
            )
        return cls(
            version=version,
            entrypoint=raw["entrypoint"],
            node_graphs={
                name: Graph.decode(g, DagNode.decode, Dependency.decode)
                for name, g in raw["node_graphs"].items()
            },
            signatures={name: Signature.decode(s) for name, s in raw["signatures"].items()},
            run_config={name: RunConfig.decode(c) for name, c in raw["run_config"].items()},
            type_env=TypeEnv.decode(raw["type_env"]),
        )
