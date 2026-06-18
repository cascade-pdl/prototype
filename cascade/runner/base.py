"""Runner base: the spawn/state abstraction shared by all runner kinds.

A runner implements two atoms — ``spawn`` (start work, return a Handle) and the
Handle's ``state`` (snapshot it). The common coordination (spawn, then wait via
an efficient await or a poll loop) is the concrete ``Runner.run`` here, written
once. See the container entrypoint env contract in the package __init__.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class RunSpec:
    run_id: str
    node_id: str
    instance_key: str
    image: str
    env: dict[str, str] = field(default_factory=dict)
    # per-node config from the ref (cpu/memory/etc.) — a NodeRunnerConfig.
    # The runner reads what it understands (EcsTaskRunner uses cpu/memory).
    runner_config: object = None
    # the ref's name — used by the ECS runner to derive the conventional
    # taskdef family + container name (must agree with provisioning's naming).
    ref_name: str | None = None


@dataclass
class TaskStatus:
    """A snapshot of a spawned task's state, polled via Handle.state().

    Structural (a Protocol would also do) — what matters is ``running`` plus the
    terminal info (``exit_code``) once it stops. The base ``Runner.run`` polls
    this until ``running`` is False, then returns ``exit_code``.
    """
    running: bool
    exit_code: int | None = None


class Handle(ABC):
    """A reference to spawned, in-flight work. ``state()`` is the primitive:
    a non-blocking snapshot of whether the work is still running and, once
    stopped, its exit code. The kind-specific check (poll ECS, check a process
    returncode, inspect an asyncio.Task) lives in each Handle subclass.

    Callers don't block in ``state()`` — the *base* ``Runner.run`` builds the
    blocking wait out of repeated ``state()`` polls plus ``asyncio.sleep``. This
    keeps the one universal mechanism (poll-until-done) written once, while each
    runner implements only the atoms (spawn, check-state)."""

    @abstractmethod
    async def state(self) -> TaskStatus:
        ...

    # Optional efficient wait. Handles backed by a real awaitable (an asyncio
    # Task, a subprocess) override this to await completion directly instead of
    # being poll-with-sleep'd — eliminating up-to-sleep_time latency. Handles
    # with no awaitable (ECS: only describe_tasks polling exists) leave it None,
    # and the base run() falls back to the state() poll loop.
    async def await_done(self) -> TaskStatus | None:
        return None


class Runner(ABC):
    """A runner implements two atoms: ``spawn`` (start the work, return a Handle)
    and the Handle's ``state`` (snapshot it). The common coordination — spawn,
    then poll state until done — is the concrete ``run`` here, written once for
    all runner kinds."""

    sleep_time: float = 0.2   # poll interval; runners override (ECS: seconds)

    @abstractmethod
    def spawn(self, spec: RunSpec) -> Handle:
        """Start the work and return a Handle. Fast — does not wait for
        completion (ECS: run_task→ARN; subprocess: launch; in-process: create
        the task). The actual waiting is polling the Handle's state."""

    async def run(self, spec: RunSpec) -> int:
        """Spawn, then wait for the handle to stop; return its exit code.

        If the handle offers an efficient await (a real awaitable underneath),
        use it — no polling latency. Otherwise fall back to the universal
        poll-state-until-done loop (the only option for e.g. ECS)."""
        handle = self.spawn(spec)
        st = await handle.await_done()
        if st is not None:
            return st.exit_code if st.exit_code is not None else 0
        while True:
            st = await handle.state()
            if not st.running:
                return st.exit_code if st.exit_code is not None else 0
            await asyncio.sleep(self.sleep_time)


class _TaskHandle(Handle):
    """Handle wrapping an asyncio.Task — for in-process / coroutine runners
    (in-process tools, the in-process dag runner). state() reports the task's
    done-ness and result."""

    def __init__(self, task: "asyncio.Task"):
        self._task = task

    async def state(self) -> TaskStatus:
        if self._task.done():
            exc = self._task.exception()
            if exc is not None:
                raise exc
            return TaskStatus(running=False, exit_code=self._task.result())
        return TaskStatus(running=True)

    async def await_done(self) -> TaskStatus | None:
        # the task is a real awaitable — await it directly, no polling latency
        result = await self._task
        return TaskStatus(running=False, exit_code=result)


class SimpleRunner(Runner):
    """Convenience base for runners whose work is one in-process coroutine
    (in-process tools, test mocks): implement async ``run_once`` and get
    ``spawn`` for free. There's nothing to poll — the coroutine *is* the work —
    so spawn wraps it in a task directly."""

    @abstractmethod
    async def run_once(self, spec: RunSpec) -> int:
        ...

    def spawn(self, spec: RunSpec) -> Handle:
        return _TaskHandle(asyncio.create_task(self.run_once(spec)))



class RunnerError(Exception):
    pass
