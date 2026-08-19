import os
import asyncio

from cascade.engine.runner.runner import Runner
from cascade.engine.run_spec import RunSpec, to_env
from cascade.store.file_store import FileConfig
from cascade.engine.runner.runner_subprocess import HandleSubprocess


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

    def _build_cmd(self, spec) -> list[str]:
        home = "/root"
        env = to_env(spec=spec)
        cmd = ["docker", "run", "--rm"]
        if self.no_pull:
            cmd += ["--pull", "never"]
        if self.map_current_user and hasattr(os, "getuid"):
            cmd += ["--user", f"{os.getuid()}:{os.getgid()}"]
            home = "/tmp"
        if self.aws_credentials_dir:
            host_aws = os.path.abspath(os.path.expanduser(self.aws_credentials_dir))
            cont_aws = os.path.join(home, ".aws")
            cmd += ["-v", f"{host_aws}:{cont_aws}:ro"]
        # A file store lives on the *host*; without a mount the container writes into its
        # own filesystem and the data is discarded on exit. Mounting the store root at the
        # same absolute path means the config that travels in CASCADE_STORE_* needs no
        # rewriting: it resolves identically on both sides.
        for host_root in _file_store_roots(spec):
            cmd += ["-v", f"{host_root}:{host_root}"]

        # resource limits, when the deployment or ref asked for them. NOTE: the
        # model does not specify units for `cpu` -- read here as whole CPUs, which
        # will need reconciling with ECS (where 1024 == 1 vCPU) in phase 5.
        if self.memory is not None:
            cmd += ["--memory", f"{self.memory}m"]
        if self.cpu is not None:
            cmd += ["--cpus", str(self.cpu)]
        env["HOME"] = home
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
    """Absolute roots of any file-backed store this spec hands the container.

    Returns at most one entry in practice: the reader and writer are subscopes of one
    deployment store, so they share a root. An S3-backed store needs no mount, which is
    why this is a dev affordance rather than the production path.
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