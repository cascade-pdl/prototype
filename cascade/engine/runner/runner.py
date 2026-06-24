from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod

from cascade.engine.runner.handle import Handle
from cascade.engine.runner.run_spec import RunSpec


class Runner(ABC):
    """A runner implements two atoms: ``spawn`` (start the work, return a Handle)
    and the Handle's ``state`` (snapshot it). The common coordination — spawn,
    then poll state until done — is the concrete ``run`` here, written once for
    all runner kinds."""

    sleep_time: int = 1

    @abstractmethod
    async def spawn(self, spec: RunSpec) -> Handle:
        """Start the work and return a Handle."""

    async def run(self, spec: RunSpec) -> int:
        """Spawn, then wait for the handle to stop; return its exit code."""
        handle = await self.spawn(spec)
        if (st := await handle.await_done()) is not None:
            return st.exit_code if st.exit_code is not None else 0
        while True:
            st = await handle.state()
            if not st.running:
                return st.exit_code if st.exit_code is not None else 0
            await asyncio.sleep(self.sleep_time)
