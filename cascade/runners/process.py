from __future__ import annotations

import os
import sys
import shutil
import asyncio
import tempfile
from asyncio.subprocess import Process
from contextlib import suppress

from cascade.protocol.bundle import wrap_args
from cascade.protocol.runner import Runner
from cascade.protocol.run_spec import Launch, RunSpec, check_launch, to_env
from cascade.protocol.coro import HandleCoro
from cascade.protocol.run_status import RunStatus


class HandleSubprocess(HandleCoro):
    """Cancelling the wait task stops nobody: the process outlives it. So ``kill``
    signals the process itself — SIGTERM, then SIGKILL if it has not gone after
    ``kill_grace`` seconds."""

    kill_grace: float = 5.0

    def __init__(self, process: Process, scratch: str | None = None):
        """``scratch``: a directory that belongs to this process alone, removed once it
        has exited, however it ends."""
        self._process = process
        self._scratch = scratch
        super().__init__(task=asyncio.create_task(self._wait_and_tidy()))

    async def _wait_and_tidy(self) -> int:
        try:
            return await self._process.wait()
        finally:
            if self._scratch is not None:
                shutil.rmtree(self._scratch, ignore_errors=True)

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
    """A command on this host, in ``cwd``.

    **Wrapped** (``wrap``): run under ``cascade node wrap``, by this same interpreter, with
    a fresh staging directory as ``CASCADE_ROOT``: the command reads
    ``$CASCADE_ROOT/inputs/<port>`` and writes ``$CASCADE_ROOT/outputs/<port>``, exactly as
    a wrapped container does under ``/cascade``. The directory is removed when the process
    ends. **Unwrapped**: the command speaks the protocol itself (the node SDK).
    """

    def __init__(self, cmd: list[str], cwd: str | None = None, wrap: bool = False):
        self.cmd = cmd
        self.cwd = cwd
        self.wrap = wrap

    def launch_for(self, spec: RunSpec) -> Launch:
        """The protocol env and the command, as a container would get them: a host
        process is held to the same budget, so moving the ref into an image changes
        nothing. The inherited host env and the staging directory are not counted."""
        return Launch(
            env=to_env(spec), command=wrap_args(self.cmd) if self.wrap else list(self.cmd)
        )

    async def spawn(self, spec: RunSpec):
        launch = self.launch_for(spec)
        check_launch(launch, who=spec.instance_id or spec.name)
        env = {**os.environ, **launch.env}
        argv, scratch = list(self.cmd), None
        if self.wrap:
            scratch = tempfile.mkdtemp(prefix="cascade-")
            env["CASCADE_ROOT"] = scratch
            argv = [sys.executable, "-m", "cascade.cli", *launch.command]
        try:
            process = await asyncio.create_subprocess_exec(*argv, cwd=self.cwd, env=env)
        except BaseException:
            if scratch is not None:
                shutil.rmtree(scratch, ignore_errors=True)
            raise
        return HandleSubprocess(process=process, scratch=scratch)
