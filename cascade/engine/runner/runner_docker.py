import asyncio
import os
from dataclasses import replace

from cascade.engine.runner.runner import Runner
from cascade.engine.runner.runner_subprocess import HandleSubprocess
from cascade.engine.run_spec import RunSpec, to_env
from cascade.store.base import Store
from cascade.store.file_store import FileConfig
from cascade.store.registry import from_config


CONTAINER_STORE_ROOT = "/cascade/store"
"""Where a host file store is mounted *inside* the container.

A fixed POSIX path rather than the host path repeated, because a host path is not a valid
mount target for a Linux container on Windows (``-v C:\\x:C:\\x`` is meaningless). So the
runner mounts host→here and rewrites the store root in the spec it hands the container.
The translation belongs here: the runner is the only component that knows a container is
involved, and scopes are untouched, so all addressing still resolves.
"""


class HandleDocker(HandleSubprocess):
    pass


class RunnerDocker(Runner):

    def __init__(
        self,
        image: str,
        no_pull: bool = True,
        extra_args: list[str] | None = None,
        map_current_user: bool = True,
        aws_credentials_dir: str | None = None,
        memory: int | None = None,
        cpu: int | None = None,
    ):
        self.image = image
        self.no_pull = no_pull
        self.extra_args = extra_args or []
        self.map_current_user = map_current_user
        self.aws_credentials_dir = aws_credentials_dir
        self.memory = memory
        self.cpu = cpu

    def _build_cmd(self, spec: RunSpec) -> list[str]:
        cmd = ["docker", "run", "--rm"]
        if self.no_pull:
            cmd += ["--pull", "never"]
        if self.map_current_user and hasattr(os, "getuid"):
            cmd += ["--user", f"{os.getuid()}:{os.getgid()}"]

        # A file store lives on the *host*; without a mount the container writes into its
        # own filesystem and the data is discarded when it exits.
        host_roots = _file_store_roots(spec)
        if host_roots:
            cmd += ["-v", f"{host_roots[0]}:{CONTAINER_STORE_ROOT}"]
            spec = _rerooted(spec, CONTAINER_STORE_ROOT)

        env = to_env(spec=spec)

        if self.aws_credentials_dir:
            # HOME is set *only* here, and only because boto looks for ~/.aws. Forcing it
            # otherwise breaks any image that pip-installed as its own user: the packages
            # sit in that user's ~/.local, and a different HOME hides them.
            home = "/tmp" if self.map_current_user and hasattr(os, "getuid") else "/root"
            host_aws = os.path.abspath(os.path.expanduser(self.aws_credentials_dir))
            cmd += ["-v", f"{host_aws}:{home}/.aws:ro"]
            env["HOME"] = home

        # resource limits, when the deployment or ref asked for them. NOTE: the model does
        # not specify units for `cpu` -- read here as whole CPUs, which will need
        # reconciling with ECS (where 1024 == 1 vCPU) in phase 5.
        if self.memory is not None:
            cmd += ["--memory", f"{self.memory}m"]
        if self.cpu is not None:
            cmd += ["--cpus", str(self.cpu)]

        for k, v in env.items():
            cmd += ["-e", f"{k}={v}"]
        cmd += self.extra_args
        cmd.append(self.image)
        return cmd

    async def spawn(self, spec: RunSpec) -> HandleDocker:
        return HandleDocker(
            process=await asyncio.create_subprocess_exec(
                *self._build_cmd(spec=spec),
            ),
        )


def _file_store_roots(spec: RunSpec) -> list[str]:
    """Absolute host roots of any file-backed store this spec hands the container.

    At most one in practice: the reader and writer are subscopes of a single deployment
    store, so they share a root. An S3-backed store needs no mount at all, which is why
    this is a development affordance rather than the production path.
    """
    roots: list[str] = []
    for store in (spec.store_in, spec.store_out):
        config = getattr(store, "config", None)
        if not isinstance(config, FileConfig):
            continue
        root = os.path.abspath(config.root)
        if root not in roots:
            roots.append(root)
    return roots


def _reroot(store: Store | None, root: str) -> Store | None:
    """The same store, addressing the same scope, under a different root."""
    if store is None or not isinstance(store.config, FileConfig):
        return store
    config = replace(store.config, root=root)
    _kind, store_cls, _config_cls = from_config(config)
    return store_cls(config)


def _rerooted(spec: RunSpec, root: str) -> RunSpec:
    return replace(
        spec,
        store_in=_reroot(spec.store_in, root),
        store_out=_reroot(spec.store_out, root),
    )
