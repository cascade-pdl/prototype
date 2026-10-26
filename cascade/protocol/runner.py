from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Iterator

from cascade.protocol.handle import Handle
from cascade.protocol.run_status import RunStatus
from cascade.protocol.run_spec import Launch, RunSpec


class Runner(ABC):
    """A runner implements two atoms: ``spawn`` (start the work, return a Handle)
    and the Handle's ``state`` (snapshot it). The common coordination — spawn,
    then poll state until done — is the concrete ``run`` here, written once for
    all runner kinds.

    **Cancelling ``run`` stops the work.** Without this, cancelling the coroutine only
    stops *waiting*: a process, a container or an ECS task carries on unsupervised. So
    if the wait is interrupted for any reason (a sibling failed, the parent was
    cancelled, Ctrl-C), the handle is killed before the interruption propagates.
    Coordination runners are runners too, so this one rule reaches every leaf of a
    nested run.
    """

    sleep_time: int = 1

    @abstractmethod
    async def spawn(self, spec: RunSpec) -> Handle:
        """Start the work and return a Handle."""

    def launch_for(self, spec: RunSpec) -> Launch | None:
        """The env and command a launch of ``spec`` would pass, or ``None`` when nothing
        crosses a process boundary (in-process runners, coordinators) or it cannot be
        known before spawning."""
        return None

    def leaves(self, spec: RunSpec) -> Iterator[tuple["Runner", RunSpec]]:
        """The runners and specs that running ``spec`` would launch, without running.

        A leaf runner is its own only leaf. Coordinators override this to walk what they
        would dispatch, so a whole run can be checked before anything starts.
        """
        yield self, spec

    async def run(self, spec: RunSpec) -> int:
        """Spawn, then wait for the handle to stop; return its exit code."""
        handle = await self.spawn(spec)
        try:
            st = await self._wait(handle)
        except BaseException as interrupted:
            try:
                await handle.kill()
            except Exception as kill_error:
                # the interruption is the story; a failed kill is a footnote to it, but
                # one worth seeing, since it means work may still be running
                interrupted.add_note(f"and stopping {spec.instance_id or spec.name} failed: {kill_error!r}")
            raise
        return st.exit_code if st.exit_code is not None else 0

    async def _wait(self, handle: Handle) -> RunStatus:
        if (st := await handle.await_done()) is not None:
            return st
        while True:
            st = await handle.state()
            if not st.running:
                return st
            await asyncio.sleep(self.sleep_time)
