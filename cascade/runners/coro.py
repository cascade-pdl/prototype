"""In-process runners: work executed here rather than spawned anywhere.

A substrate like any other — the choice it makes is "this process" — which is why these are
runners and the base adapter they build on is protocol.
"""
from __future__ import annotations

from typing import Any, Awaitable, Callable, Mapping, Sequence

from cascade.protocol.coro import RunnerCoroBase
from cascade.protocol.run_spec import RunSpec


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
