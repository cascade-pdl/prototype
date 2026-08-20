"""The ``echo`` runner: reports what it was asked to run, writes stub outputs, succeeds.

``RunnerKind.echo`` had no implementation, so the registry could not cover its own
vocabulary. This closes that, and gives the executor milestones a runner needing no
docker, no subprocess and no filesystem.

It writes a stub artifact for each of its output ports, which is what makes a
multi-node dag testable: a downstream node has something to read. A stub respects the
port's declared *depth*, so a ``T[]`` port yields a list and a downstream node can fan
over it. The ports and their depths come from the plan's ``Signature`` at *construction* — a signature is per-runnable and so
is the runner, so nothing about this varies per instance. Where the stubs land is
decided entirely by the scope of ``spec.store_out``; echo neither knows nor cares
where that is.
"""
from __future__ import annotations

from typing import Any, Awaitable, Mapping

from cascade.protocol.coro import RunnerCoroBase
from cascade.protocol.run_spec import RunSpec


class RunnerEcho(RunnerCoroBase):
    def __init__(
        self,
        message: str = "Hello world!",
        outputs: Mapping[str, int] | None = None,
    ):
        """``outputs`` maps each output port to its declared array depth, so a stub
        for a ``T[]`` port is a *list* — otherwise a downstream node could not fan
        over it, and the stub would not satisfy the type the plan promises."""
        self.message = message
        self.outputs = dict(outputs or {})
        self._name = ""
        self._instance = None

    def _stub(self, port: str, depth: int) -> Any:
        if depth <= 0:
            return {
                "echo": self.message,
                "port": port,
                "runnable": self._name,
                "instance": self._instance,
            }
        return [self._stub(port, depth - 1) for _ in range(3)]

    async def _echo(self, spec: RunSpec) -> int:
        where = spec.instance_id or spec.node_id or spec.name
        print(f"[echo {where}] {self.message}")
        self._name = spec.name
        self._instance = spec.instance_id
        if spec.store_out is not None:
            for port, depth in self.outputs.items():
                spec.store_out.put_json(port, self._stub(port, depth))
        return 0

    def _invoke(self, spec: RunSpec) -> Awaitable[int]:
        return self._echo(spec)