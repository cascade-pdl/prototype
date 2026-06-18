"""Runner registry and deployment-satisfaction checks.

The registry resolves a runner *kind* (from a ref) to a runner *instance*,
lazily, using the deployment config. check_deployment_satisfies fails fast when
the deployment can't serve a required kind or pairs it with an unreachable store.
"""

from __future__ import annotations

from .base import RunnerError
from .subprocess import SubprocessRunner
from .ecs_task import EcsTaskRunner
from .echo import EchoRunner


class RunnerRegistry:
    """Resolves a RunnerKind to a Runner instance, building each lazily from the
    deployment config and caching it (so expensive init — boto3 sessions, etc. —
    happens once per kind, not per node instance).

    Deployment config is per-environment and supplied at construction; it is NOT
    part of the pipeline. The pipeline only declares *which kind* each ref needs;
    this registry supplies the *configured instance* for the current deployment.

    For local subprocess runs the store must be reachable inside the container,
    so the registry is also given the store_root (the host FileStore dir) and any
    base docker args (e.g. credential mounts) to apply to subprocess runs.
    """

    def __init__(self, deployment, store_root: str | None = None,
                 subprocess_extra_args: list[str] | None = None,
                 project_name: str | None = None):
        from ..runners_config import RunnerKind
        self.deployment = deployment
        self.store_root = store_root
        self.subprocess_extra_args = subprocess_extra_args or []
        self.project_name = project_name
        self._cache: dict = {}
        self._RunnerKind = RunnerKind

    def get(self, kind) -> Runner:
        if kind in self._cache:
            return self._cache[kind]
        runner = self._build(kind)
        self._cache[kind] = runner
        return runner

    def _build(self, kind) -> Runner:
        K = self._RunnerKind
        if kind == K.echo:
            return EchoRunner()
        if kind == K.subprocess:
            extra = list(self.subprocess_extra_args)
            sd = self.deployment.subprocess if self.deployment else None
            if sd:
                extra = sd.extra_args + extra
                return SubprocessRunner(
                    store_root=self.store_root,
                    container_store=sd.container_store,
                    no_pull=sd.no_pull,
                    extra_args=extra,
                    map_current_user=sd.map_current_user,
                    aws_credentials_host=sd.aws_credentials_host,
                    aws_credentials_container=sd.aws_credentials_container,
                    home=sd.home,
                )
            return SubprocessRunner(
                store_root=self.store_root,
                extra_args=extra,
            )
        if kind == K.ecs_task:
            if not (self.deployment and self.deployment.ecs):
                raise RunnerError(
                    "pipeline requires runner kind 'ecs-task' but the deployment "
                    "config provides no 'ecs' section (cluster, region, ...)"
                )
            return EcsTaskRunner(self.deployment.ecs, project_name=self.project_name)
        raise RunnerError(f"no runner implementation for kind '{kind}'")



def check_deployment_satisfies(required_kinds, deployment) -> list[str]:
    """Return a list of human-readable problems if the deployment can't satisfy
    the pipeline's runner kinds, or pairs them with an unreachable store. Empty
    list = OK. Used for fail-fast validation before a run starts."""
    problems = []
    for kind in required_kinds:
        if not deployment.provides(kind):
            problems.append(
                f"pipeline requires runner kind '{getattr(kind, 'value', kind)}' "
                f"but the deployment config does not provide it"
            )

    # store/runner reachability: a local FileStore is only reachable by runners
    # that share the engine's filesystem (subprocess). An ECS task cannot reach
    # a path on the engine host, so FileStore + ecs-task is a silent breakage —
    # catch it here, before launching anything, rather than as a cryptic
    # per-task failure deep in the run.
    from ..store_config import StoreKind
    store = getattr(deployment, "store", None)
    if store is not None and store.kind == StoreKind.file:
        for kind in required_kinds:
            if not getattr(kind, "shares_engine_filesystem", True):
                problems.append(
                    f"pipeline uses runner kind '{getattr(kind, 'value', kind)}', "
                    f"which runs nodes off the engine host and cannot reach a local "
                    f"file store; use an S3 store in the deployment config "
                    f"(or a shared filesystem, not yet supported)"
                )
    return problems
