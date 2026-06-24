import os
import asyncio

from cascade.engine.runner.runner import Runner
from cascade.engine.runner.run_spec import RunSpec, to_env
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
    ):
        self.image = image
        self.no_pull = no_pull
        self.extra_args = extra_args or []
        self.map_current_user = map_current_user
        self.aws_credentials_dir = aws_credentials_dir

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
