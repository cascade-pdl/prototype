"""The ``echo`` runner: reports what it was asked to run and succeeds.

``RunnerKind.echo`` had no implementation, so the registry could not cover its own
vocabulary. This closes that, and gives the executor milestones a runner that needs
no docker, no subprocess and no filesystem.

Deliberately minimal: it writes **no outputs**, because a ``RunSpec`` does not carry
the signature that would tell it which ports to produce. Once the executor passes
bindings (Phase 1), an echo ref that emits stub outputs becomes possible and will
be needed to exercise a multi-node dag end to end.
"""
from __future__ import annotations

from typing import Awaitable

from cascade.engine.runner.runner_coro import RunnerCoroBase
from cascade.engine.runner.run_spec import RunSpec


class RunnerEcho(RunnerCoroBase):
    def __init__(self, message: str = "Hello world!"):
        self.message = message

    async def _echo(self, spec: RunSpec) -> int:
        where = spec.instance_id or spec.node_id or spec.name
        print(f"[echo {where}] {self.message}")
        return 0

    def _invoke(self, spec: RunSpec) -> Awaitable[int]:
        return self._echo(spec)