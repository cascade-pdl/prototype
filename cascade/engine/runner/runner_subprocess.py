from __future__ import annotations

import os
import json
import asyncio
from asyncio.subprocess import Process

from cascade.engine.runner.runner import Runner
from cascade.engine.runner.run_status import RunStatus
from cascade.engine.run_spec import RunSpec, to_env
from cascade.engine.runner.runner_coro import HandleCoro


class HandleSubprocess(HandleCoro):
    def __init__(self, process: Process):
        self._process = process
        super().__init__(task=asyncio.create_task(self._process.wait()))


class RunnerSubprocess(Runner):
    def __init__(self, cmd: list[str], cwd: str | None = None):
        self.cmd = cmd
        self.cwd = cwd

    async def spawn(self, spec: RunSpec):
        return HandleSubprocess(
            process=await asyncio.create_subprocess_exec(
                *self.cmd,
                cwd=self.cwd,
                env={**os.environ, **to_env(spec)},
            ),
        )