import asyncio
from typing import Awaitable, Callable, Sequence, Any, Mapping

from cascade.engine.runner.runner import Runner
from cascade.engine.runner.handle import Handle
from cascade.engine.runner.run_status import RunStatus
from cascade.engine.runner.run_spec import RunSpec


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


class RunnerCoro(Runner):

    def __init__(
        self,
        coro: Callable[..., Awaitable[Any]],
        args: Sequence[Any] | None = None,
        kwas: Mapping[str, Any] | None = None,
    ):
        self.coro = coro
        self.args = args or ()
        self.kwas = kwas or {}

    async def spawn(self, spec: RunSpec) -> HandleCoro:
        return HandleCoro(
            task=asyncio.create_task(
                self.coro(
                    *self.args,
                    **self.kwas,
                ),
            ),
        )
