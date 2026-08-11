"""The ``echo`` runner: reports what it was asked to run, writes stub outputs, succeeds.

``RunnerKind.echo`` had no implementation, so the registry could not cover its own
vocabulary. This closes that, and gives the executor milestones a runner needing no
docker, no subprocess and no filesystem.

It writes a stub artifact for each of its output ports, which is what makes a
multi-node dag testable: a downstream node has something to read. The port names come
from the plan's ``Signature`` at *construction* — a signature is per-runnable and so
is the runner, so nothing about this varies per instance. Where the stubs land is
decided entirely by the scope of ``spec.store_out``; echo neither knows nor cares
where that is.
"""
from __future__ import annotations

import json
from typing import Awaitable, Iterable

from cascade.engine.runner.runner_coro import RunnerCoroBase
from cascade.engine.run_spec import RunSpec


class RunnerEcho(RunnerCoroBase):
    def __init__(self, message: str = "Hello world!", outputs: Iterable[str] = ()):
        self.message = message
        self.outputs = tuple(outputs)

    async def _echo(self, spec: RunSpec) -> int:
        where = spec.instance_id or spec.node_id or spec.name
        print(f"[echo {where}] {self.message}")
        if spec.store_out is not None:
            for port in self.outputs:
                spec.store_out.put_json(
                    port,
                    {
                        "echo": self.message,
                        "port": port,
                        "runnable": spec.name,
                        "instance": spec.instance_id,
                    },
                )
        return 0

    def _invoke(self, spec: RunSpec) -> Awaitable[int]:
        return self._echo(spec)