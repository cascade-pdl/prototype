"""In-process runners: work that is a coroutine rather than a process.

The task/handle plumbing is written once in ``RunnerCoroBase``; subclasses decide
only *how the callable is invoked*. That split exists because two genuinely
different contracts share the plumbing:

- ``RunnerCoro`` passes the ``RunSpec`` to the callable. This is the contract for
  engine coordination (a dag or a fanned region, which need their bindings) and for
  any future in-process ref. It is what makes a local dag runner substitutable for
  a remote one — both receive the same input.
- ``RunnerAwaitable`` ignores the spec and calls a pre-bound callable. This is for
  side-effect-free awaitables (``asyncio.sleep``), mocks and test doubles, where
  there is nothing to bind.

Neither is a specialisation of the other, so neither subclasses the other: a class
that silently changed its callable's arguments would not be substitutable for its
parent.

The coroutine's return value becomes the handle's exit code; ``None`` is treated as
success by ``Runner.run``.
"""
from __future__ import annotations

import asyncio
from abc import abstractmethod
from typing import Any, Awaitable, Callable, Mapping, Sequence

from cascade.protocol.runner import Runner
from cascade.protocol.handle import Handle
from cascade.engine.runner.run_status import RunStatus
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


class RunnerCoro(RunnerCoroBase):
    """Calls ``coro(spec)``. The contract for engine coordination and in-process
    refs — anything that needs to know what it is running."""

    def __init__(self, coro: Callable[[RunSpec], Awaitable[Any]]):
        self.coro = coro

    def _invoke(self, spec: RunSpec) -> Awaitable[Any]:
        return self.coro(spec)


class RunnerAwaitable(RunnerCoroBase):
    """Calls a pre-bound ``coro(*args, **kwas)``, ignoring the spec. For plain
    awaitables and test doubles that have nothing to bind."""

    def __init__(
        self,
        coro: Callable[..., Awaitable[Any]],
        args: Sequence[Any] | None = None,
        kwas: Mapping[str, Any] | None = None,
    ):
        self.coro = coro
        self.args = args or ()
        self.kwas = kwas or {}

    def _invoke(self, spec: RunSpec) -> Awaitable[Any]:
        return self.coro(*self.args, **self.kwas)
