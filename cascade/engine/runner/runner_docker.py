import os
import asyncio

from cascade.engine.runner.runner import Runner
from cascade.engine.run_spec import RunSpec, to_env
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