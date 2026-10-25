"""Backends: the places a deployment can run work.

A backend is one entry of the deployment's ordered ``backends:`` list. Its ``kind`` says
what sort of place it is — ``local`` (the executor's own host) or ``ecs`` — and several
backends may share a kind: ``ecs-cpu`` on Fargate Spot beside ``ecs-gpu`` on an EC2
capacity provider, on one cluster. Placement reads ``max`` and ``provides``; the runner
builder reads ``config``; the concurrency limiter will read ``concurrency``.

``max`` is policy, not hardware: setting it below what a laptop really has is how a
deployment says "only small work runs here".

These are data definitions only. They live below ``cascade.runners`` and
``cascade.deployment`` so that both can read them: the deployment file decodes into them,
and the builder turns a placed ref plus its backend into a runner.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, ClassVar, Mapping, Self

from cascade.model.resources import Resources
from cascade.model.runner_kinds import RunnerKind


class BackendError(ValueError):
    """A backend entry that does not describe a usable place."""


class BackendKind(str, Enum):
    local = "local"
    ecs = "ecs"


def _reject_unknown(what: str, raw: Mapping[str, Any], known: set[str]) -> None:
    unknown = set(raw) - known
    if unknown:
        raise BackendError(f"{what}: unknown field(s) {sorted(unknown)}")


@dataclass(frozen=True)
class LocalConfig:
    """Settings for running on the executor's host.

    Machine facts, which is why they are here and not on a ref: a ref naming a path on
    somebody's laptop, or the user a container runs as, would make the pipeline
    unportable.
    """

    aws_credentials_dir: str | None = None
    """Host path mounted at ``~/.aws`` in docker containers."""
    map_current_user: bool = True
    """Run containers as the invoking user, so files they write are owned by them."""
    no_pull: bool = True
    """Use locally built images rather than pulling."""
    bundle: str | None = None
    """Host path of the unpacked cascade bundle (``bundle/build.sh`` writes
    ``dist/bundle/opt-cascade``), mounted read-only into every wrapped docker container.
    Relative to the pipeline's directory. Required when a wrapped docker ref is placed
    here."""

    @classmethod
    def decode(cls, raw: Mapping[str, Any]) -> Self:
        _reject_unknown(
            "local config", raw, {"aws_credentials_dir", "map_current_user", "no_pull", "bundle"}
        )
        return cls(
            aws_credentials_dir=raw.get("aws_credentials_dir"),
            map_current_user=raw.get("map_current_user", True),
            no_pull=raw.get("no_pull", True),
            bundle=raw.get("bundle"),
        )

    def encode(self) -> dict[str, Any]:
        return {
            "aws_credentials_dir": self.aws_credentials_dir,
            "map_current_user": self.map_current_user,
            "no_pull": self.no_pull,
            "bundle": self.bundle,
        }


@dataclass(frozen=True)
class EcsConfig:
    """Where and how ECS tasks run. Read by the ECS runner (not built yet).

    Exactly one of ``launch_type`` (Fargate) and ``capacity_provider`` (e.g. an EC2 GPU
    auto-scaling group) says what the tasks run on; Fargate when neither is given.
    """

    LAUNCH_TYPES: ClassVar[frozenset[str]] = frozenset({"FARGATE", "FARGATE_SPOT"})

    cluster: str
    subnets: tuple[str, ...]
    region: str | None = None
    security_groups: tuple[str, ...] = ()
    assign_public_ip: bool = False
    task_role: str | None = None
    execution_role: str | None = None
    log_group: str | None = None
    launch_type: str | None = None
    capacity_provider: str | None = None
    tags: Mapping[str, str] = field(default_factory=dict)

    @classmethod
    def decode(cls, raw: Mapping[str, Any]) -> Self:
        _reject_unknown("ecs config", raw, {
            "cluster", "subnets", "region", "security_groups", "assign_public_ip",
            "task_role", "execution_role", "log_group", "launch_type",
            "capacity_provider", "tags",
        })
        missing = {"cluster", "subnets"} - set(raw)
        if missing:
            raise BackendError(f"ecs config needs {sorted(missing)}")
        launch_type, provider = raw.get("launch_type"), raw.get("capacity_provider")
        if launch_type is not None and provider is not None:
            raise BackendError("ecs config: give launch_type or capacity_provider, not both")
        if launch_type is not None and launch_type not in cls.LAUNCH_TYPES:
            raise BackendError(
                f"ecs config: launch_type {launch_type!r} is not one of {sorted(cls.LAUNCH_TYPES)}"
            )
        return cls(
            cluster=raw["cluster"],
            subnets=tuple(raw["subnets"]),
            region=raw.get("region"),
            security_groups=tuple(raw.get("security_groups") or ()),
            assign_public_ip=raw.get("assign_public_ip", False),
            task_role=raw.get("task_role"),
            execution_role=raw.get("execution_role"),
            log_group=raw.get("log_group"),
            launch_type=launch_type,
            capacity_provider=provider,
            tags=dict(raw.get("tags") or {}),
        )

    def encode(self) -> dict[str, Any]:
        return {
            "cluster": self.cluster,
            "subnets": list(self.subnets),
            "region": self.region,
            "security_groups": list(self.security_groups),
            "assign_public_ip": self.assign_public_ip,
            "task_role": self.task_role,
            "execution_role": self.execution_role,
            "log_group": self.log_group,
            "launch_type": self.launch_type,
            "capacity_provider": self.capacity_provider,
            "tags": dict(self.tags),
        }


BackendConfig = LocalConfig | EcsConfig

CONFIGS: Mapping[BackendKind, type[LocalConfig] | type[EcsConfig]] = {
    BackendKind.local: LocalConfig,
    BackendKind.ecs: EcsConfig,
}

RUNS: Mapping[BackendKind, frozenset[RunnerKind]] = {
    BackendKind.local: frozenset({RunnerKind.docker, RunnerKind.subprocess}),
    BackendKind.ecs: frozenset({RunnerKind.docker}),
}
"""Which ref kinds each kind of backend can run. A subprocess is a command on the
executor's host, so only ``local`` runs one; ``echo`` is a stub and is never placed."""


@dataclass(frozen=True)
class Backend:
    name: str
    kind: BackendKind
    max: Resources | None
    """The largest task accepted. ``None`` only for :func:`uncapped_local`: a deployment
    file must state it."""
    config: BackendConfig
    provides: frozenset[str] | None = frozenset()
    """Capabilities offered. ``None`` only for :func:`uncapped_local`: with no deployment
    there is no policy to check a ref's ``requires`` against."""
    concurrency: int | None = None
    """How many tasks may run here at once; ``None`` is unlimited. Required on
    ``ecs``: Fargate does not queue, so past the account quota RunTask just fails."""

    def runs(self, kind: RunnerKind) -> bool:
        return kind in RUNS[self.kind]

    @classmethod
    def decode(cls, raw: Mapping[str, Any]) -> Self:
        if not isinstance(raw, Mapping) or "name" not in raw:
            raise BackendError(f"a backend needs a name: {raw!r}")
        name = raw["name"]
        _reject_unknown(
            f"backend {name!r}", raw,
            {"name", "kind", "max", "provides", "concurrency", "config"},
        )
        try:
            kind = BackendKind(raw.get("kind"))
        except ValueError:
            raise BackendError(
                f"backend {name!r}: kind {raw.get('kind')!r} is not one of "
                f"{[k.value for k in BackendKind]}"
            )
        if "max" not in raw:
            raise BackendError(
                f"backend {name!r} needs `max: {{cpu, memory}}`: the largest task it accepts"
            )
        concurrency = raw.get("concurrency")
        if concurrency is None and kind is BackendKind.ecs:
            raise BackendError(
                f"backend {name!r}: an ecs backend needs `concurrency` — Fargate does not "
                "queue, so an uncapped fan fails at the account quota"
            )
        if concurrency is not None and (
            isinstance(concurrency, bool) or not isinstance(concurrency, int) or concurrency < 1
        ):
            raise BackendError(f"backend {name!r}: concurrency must be a positive integer")
        try:
            config = CONFIGS[kind].decode(raw.get("config") or {})
            limit = Resources.decode(raw["max"])
        except ValueError as e:
            raise BackendError(f"backend {name!r}: {e}")
        return cls(
            name=name,
            kind=kind,
            max=limit,
            config=config,
            provides=frozenset(raw.get("provides") or ()),
            concurrency=concurrency,
        )

    def encode(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "kind": self.kind.value,
            "max": self.max.encode() if self.max is not None else None,
            "provides": sorted(self.provides) if self.provides is not None else None,
            "concurrency": self.concurrency,
            "config": self.config.encode(),
        }


def uncapped_local() -> Backend:
    """The host, with no limits: what runs when there is no deployment at all.

    Only for that case — tests, and ``cascade run --backend`` where flags replace the
    deployment. No ``max`` and no ``provides`` to check: there is no policy. A deployment file always lists its backends; one without a ``local``
    entry means nothing runs on the executor's host.
    """
    return Backend(
        name="local", kind=BackendKind.local, max=None, config=LocalConfig(), provides=None
    )
