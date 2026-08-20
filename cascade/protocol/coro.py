"""Running a coroutine as a ``Runner`` — the adapter both halves of the system need.

``HandleCoro`` and ``RunnerCoroBase`` know nothing about any substrate: they turn "an async
function that returns an exit code" into the ``Runner``/``Handle`` pair. That is why they
sit in ``protocol`` rather than with the runners — *coordination* uses them (a dag and a fan
each run as a coroutine) and so do *substrates* (echo, and the subprocess handle). Leaving
them among the substrate implementations is what made ``engine`` and ``engine.runner``
depend on each other.

Concrete in-process runners built on this — ``RunnerCoro``, ``RunnerAwaitable`` — live in
``cascade.runners.coro``, because those *are* substrate: they choose to run the work here,
in this process.
"""
from __future__ import annotations

import asyncio
from abc import abstractmethod
from typing import Any, Awaitable

from cascade.protocol.runner import Runner
from cascade.protocol.handle import Handle
from cascade.protocol.run_status import RunStatus
from cascade.protocol.run_spec import RunSpec


class HandleCoro(Handle):
    def __init__(self, task: asyncio.Task):
        self.task = task

    async def state(self) -> RunStatus:
        if self.task.done():
            exc = self.task.exception()
            if exc is not None:
                raise exc
            return RunStatus(
                running=False,
                exit_code=self.task.result(),
            )
        return RunStatus(running=True)

    async def await_done(self) -> RunStatus | None:
        result = await self.task
        return RunStatus(
            running=False,
            exit_code=result,
        )


class RunnerCoroBase(Runner):
    """Spawns the coroutine returned by ``_invoke`` as a task. Subclasses supply
    the invocation; everything else is shared."""

    @abstractmethod
    def _invoke(self, spec: RunSpec) -> Awaitable[Any]:
        """Return the awaitable to run for this spec (do not await it here)."""

    async def spawn(self, spec: RunSpec) -> HandleCoro:
        return HandleCoro(task=asyncio.create_task(self._invoke(spec)))
