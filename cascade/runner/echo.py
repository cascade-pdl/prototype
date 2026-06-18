"""Echo runner: a no-op for dry runs / tests. Prints what it would launch."""

from __future__ import annotations

import asyncio

from .base import Runner, Handle, RunSpec, _TaskHandle


class EchoRunner(Runner):
    """A no-op runner for tests/dry-runs: prints what it would launch and
    succeeds. Lets you exercise the engine without Docker."""

    def __init__(self, log: list[str] | None = None):
        self.log = log if log is not None else []

    def spawn(self, spec: RunSpec) -> Handle:
        async def _go() -> int:
            line = (
                f"[echo] would run {spec.image} "
                f"node={spec.node_id} instance={spec.instance_key} "
                f"inputs={spec.env.get('CASCADE_INPUT_KEYS')}"
            )
            self.log.append(line)
            print(line)
            return 0
        return _TaskHandle(asyncio.create_task(_go()))


# --------------------------------------------------------------------------- #
# HookedRunner — runs the node-side translation hooks around an inner runner.
