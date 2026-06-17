"""Runner vocabulary, per-node config, and deployment config.

Two distinct kinds of configuration, deliberately separated:

  - **Per-node config** (in the pipeline YAML, on the ref): intrinsic needs of
    the node — how much cpu/memory it wants. Travels with the pipeline because
    it is a property of the node regardless of where it runs. Discriminated by
    runner ``kind`` so each kind validates its own schema.

  - **Deployment config** (NOT in the pipeline — supplied at run time): where /
    how runners connect for *this* environment — the ECS cluster, region, the
    taskiq endpoint. Kept out of the pipeline so the same pipeline runs in dev,
    prod, or a contributor's setup unchanged.

The two meet at execution: a runner is *instantiated* with deployment config
(once, lazily, cached) and its ``run`` is *called* with the node's per-node
config.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class RunnerKind(str, Enum):
    """The fixed vocabulary of runner kinds. The YAML's ``ref.runner`` is one of
    these; ``validate-refs`` checks membership. New kinds are added here."""
    subprocess = "subprocess"
    ecs_task = "ecs-task"
    echo = "echo"            # no-op, for dry runs / tests


# --------------------------------------------------------------------------- #
# Per-node config (on the ref, in the pipeline) — discriminated by kind.
# Only *intrinsic node needs* belong here (cpu/memory/timeout). Deployment
# wiring (cluster, region, endpoint) does NOT — that is DeploymentConfig.
# --------------------------------------------------------------------------- #
@dataclass
class SubprocessNodeConfig:
    kind: RunnerKind = RunnerKind.subprocess
    memory: int | None = None       # optional --memory limit (MB); intrinsic


@dataclass
class EcsTaskNodeConfig:
    kind: RunnerKind = RunnerKind.ecs_task
    cpu: int | None = None          # ECS task cpu units (e.g. 2048)
    memory: int | None = None       # ECS task memory (MB, e.g. 8192)
    timeout: float | None = None    # seconds
    # (future: per-node IAM role override, networking, etc.)


@dataclass
class EchoNodeConfig:
    kind: RunnerKind = RunnerKind.echo


# the discriminated union of per-node configs
NodeRunnerConfig = SubprocessNodeConfig | EcsTaskNodeConfig | EchoNodeConfig

_NODE_CONFIG_BY_KIND = {
    RunnerKind.subprocess: SubprocessNodeConfig,
    RunnerKind.ecs_task: EcsTaskNodeConfig,
    RunnerKind.echo: EchoNodeConfig,
}


def parse_node_config(kind: RunnerKind, raw: dict[str, Any] | None) -> NodeRunnerConfig:
    """Build the per-node config for a kind from a raw dict, validating that the
    keys belong to that kind's schema (discriminated validation)."""
    cls = _NODE_CONFIG_BY_KIND[kind]
    raw = dict(raw or {})
    raw.pop("kind", None)  # the discriminator is implied by the ref's runner kind
    allowed = {f for f in cls.__dataclass_fields__ if f != "kind"}
    unknown = set(raw) - allowed
    if unknown:
        raise ValueError(
            f"runner config for kind '{kind.value}' has unknown field(s): "
            f"{sorted(unknown)}; allowed: {sorted(allowed)}"
        )
    return cls(**raw)


# --------------------------------------------------------------------------- #
# Ref.runner — either a bare kind or a kind + per-node config
# --------------------------------------------------------------------------- #
@dataclass
class RunnerSpec:
    """What a ref declares for its runner: the kind, plus optional per-node
    config. A bare ``runner: subprocess`` in YAML normalises to RunnerSpec with
    a default config for that kind."""
    kind: RunnerKind
    config: NodeRunnerConfig = None  # type: ignore[assignment]

    def __post_init__(self):
        if self.config is None:
            self.config = _NODE_CONFIG_BY_KIND[self.kind]()


# --------------------------------------------------------------------------- #
# Deployment config (separate file / runtime; NOT in the pipeline)
# --------------------------------------------------------------------------- #
@dataclass
class EcsDeployment:
    cluster: str
    region: str | None = None
    task_role: str | None = None
    subnets: list[str] = field(default_factory=list)
    security_groups: list[str] = field(default_factory=list)
    log_group: str | None = None


@dataclass
class SubprocessDeployment:
    # local docker: optional extra args, container store mount point
    container_store: str = "/store"
    no_pull: bool = True
    extra_args: list[str] = field(default_factory=list)


@dataclass
class DeploymentConfig:
    """Per-environment runner wiring, keyed by runner kind. Supplied at run time
    (a --runner-config file / env / CLI), never embedded in the pipeline."""
    ecs: EcsDeployment | None = None
    subprocess: SubprocessDeployment | None = None

    def provides(self, kind: RunnerKind) -> bool:
        """Does this deployment supply what the given runner kind needs?"""
        if kind == RunnerKind.ecs_task:
            return self.ecs is not None
        if kind == RunnerKind.subprocess:
            return True   # subprocess works with defaults even if unspecified
        if kind == RunnerKind.echo:
            return True
        return False

    @classmethod
    def from_dict(cls, raw: dict[str, Any] | None) -> "DeploymentConfig":
        raw = raw or {}
        runners = raw.get("runners", raw)  # accept {runners: {...}} or {...}
        ecs = None
        if runners.get("ecs") or runners.get("ecs-task"):
            e = runners.get("ecs") or runners.get("ecs-task")
            ecs = EcsDeployment(
                cluster=e["cluster"],
                region=e.get("region"),
                task_role=e.get("task_role") or e.get("aws_role"),
                subnets=e.get("subnets") or [],
                security_groups=e.get("security_groups") or [],
                log_group=e.get("log_group"),
            )
        sub = None
        if runners.get("subprocess"):
            s = runners["subprocess"]
            sub = SubprocessDeployment(
                container_store=s.get("container_store", "/store"),
                no_pull=s.get("no_pull", True),
                extra_args=s.get("extra_args") or [],
            )
        return cls(ecs=ecs, subprocess=sub)
