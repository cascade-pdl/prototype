from __future__ import annotations

import os
import asyncio
from asyncio.subprocess import Process
from contextlib import suppress

from cascade.protocol.runner import Runner
from cascade.protocol.run_spec import RunSpec, to_env
from cascade.protocol.coro import HandleCoro
from cascade.protocol.run_status import RunStatus


class HandleSubprocess(HandleCoro):
    """Cancelling the wait task stops nobody: the process outlives it. So ``kill``
    signals the process itself — SIGTERM, then SIGKILL if it has not gone after
    ``kill_grace`` seconds."""

    kill_grace: float = 5.0

    def __init__(self, process: Process):
        self._process = process
        super().__init__(task=asyncio.create_task(self._process.wait()))

    async def kill(self) -> RunStatus:
        if self._process.returncode is None:
            with suppress(ProcessLookupError):
                self._process.terminate()
            try:
                await asyncio.wait_for(self._process.wait(), self.kill_grace)
            except TimeoutError:
                with suppress(ProcessLookupError):
                    self._process.kill()
                await self._process.wait()
        # the wait task finishes by itself once the process has; settle it so nothing
        # is left pending
        await asyncio.wait({self.task})
        return RunStatus(running=False, exit_code=self._process.returncode)


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
