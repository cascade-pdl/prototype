"""Concurrency limits: slots a leaf holds while its work runs.

Coordination (``DagRunner``, ``FanRunner``, ``run_all``) decides *when* work may start and
starts everything that may. *How many* run at once is decided here, and only for leaves:
a dag or a fan is a coroutine in the executor and uses nothing, and a coordinator holding
a slot per lane would deadlock once its lanes' leaves wait for slots of their own.

A run has one ``Limiter``, bound into the ref builder, so a limit counts across every fan
and every nesting level of the run, not per node. It holds two kinds of pool:

- a **backend** pool per backend with a ``concurrency``: how many tasks run there at once
  (Fargate does not queue, so past the account quota ``RunTask`` simply fails);
- a **named** pool per entry of the deployment's ``limits:``: a class of work wherever it
  runs, or a shared outside resource (an API, a database, a licence). A ref names its
  class with ``limit:``; the deployment says how many.

A leaf takes **all its pools in one step**: it waits until each has room, then takes them
together, so it never holds one slot while waiting for another, and two pools cannot
deadlock each other. The slot is held from before ``spawn`` until ``run`` has returned or
re-raised, and ``run`` kills its handle before re-raising, so a freed slot always means
the work has stopped. A leaf cancelled while waiting has started nothing.

**Order.** Waiters are served in arrival order, skipping any whose pools are not all free:
when a slot frees, the earliest waiter that now fits takes it. A waiter needing two pools
can be passed by later ones needing one; nothing outside this module may rely on the
order, so a different queue (priorities, aging) can replace it here alone.

In-process: one executor launches and watches every leaf of a run, so one counter sees
them all. Sharing a budget across executors would need leases in a shared store instead.
"""
from __future__ import annotations

import asyncio
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator, Iterator, Mapping, NamedTuple

from cascade.protocol.handle import Handle
from cascade.protocol.run_spec import Launch, RunSpec
from cascade.protocol.runner import Runner


class LimiterError(Exception):
    """A request for a pool the limiter does not have."""


class Pool(NamedTuple):
    kind: str
    """``backend`` or ``limit``: the two share no namespace, so a backend and a named
    limit may have the same name."""
    name: str

    def __str__(self) -> str:
        return f"{self.kind} {self.name!r}"


def backend_pool(name: str) -> Pool:
    return Pool("backend", name)


def named_pool(label: str) -> Pool:
    return Pool("limit", label)


@dataclass(frozen=True)
class SlotRequest:
    """What one leaf asks for: every pool it needs, and who it is (for messages, and for
    any queueing policy that wants to know)."""

    pools: tuple[Pool, ...]
    who: str = ""


class Limiter:
    def __init__(self, capacities: Mapping[Pool, int]):
        bad = {str(p): c for p, c in capacities.items() if c < 1}
        if bad:
            raise LimiterError(f"capacities must be at least 1: {bad}")
        self._capacity = dict(capacities)
        self._used = {p: 0 for p in capacities}
        self._waiting: deque[tuple[tuple[Pool, ...], asyncio.Future[None]]] = deque()

    def capacity(self, pool: Pool) -> int | None:
        return self._capacity.get(pool)

    def in_use(self, pool: Pool) -> int:
        return self._used[pool]

    @asynccontextmanager
    async def slot(self, request: SlotRequest) -> AsyncIterator[None]:
        """Hold one slot in every pool of ``request`` for the body of the ``with``."""
        pools = tuple(dict.fromkeys(request.pools))
        unknown = [str(p) for p in pools if p not in self._capacity]
        if unknown:
            raise LimiterError(f"{request.who or 'a leaf'} asks for unknown pool(s) {unknown}")
        granted: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._waiting.append((pools, granted))
        self._grant()
        try:
            await granted
        except BaseException:
            if granted.done() and not granted.cancelled():
                # granted, then interrupted before it could resume: give the slot back
                self._release(pools)
            else:
                self._waiting.remove((pools, granted))
            raise
        try:
            yield
        finally:
            self._release(pools)

    def _fits(self, pools: tuple[Pool, ...]) -> bool:
        return all(self._used[p] < self._capacity[p] for p in pools)

    def _grant(self) -> None:
        # check and take with no await between, so a grant is atomic across its pools
        for entry in list(self._waiting):
            pools, granted = entry
            if self._fits(pools):
                for p in pools:
                    self._used[p] += 1
                self._waiting.remove(entry)
                granted.set_result(None)

    def _release(self, pools: tuple[Pool, ...]) -> None:
        for p in pools:
            self._used[p] -= 1
        self._grant()


class LimitedRunner(Runner):
    """A leaf runner that runs only while holding its slots.

    Transparent otherwise: its leaves and launches are the inner runner's, so a run is
    checked before it starts exactly as if there were no limit.
    """

    def __init__(self, inner: Runner, limiter: Limiter, pools: tuple[Pool, ...]):
        self.inner = inner
        self.limiter = limiter
        self.pools = pools

    async def run(self, spec: RunSpec) -> int:
        request = SlotRequest(self.pools, who=spec.instance_id or spec.name)
        async with self.limiter.slot(request):
            return await self.inner.run(spec)

    async def spawn(self, spec: RunSpec) -> Handle:
        # a spawn here would start work outside the slot, which is the one thing a limit
        # must not allow; ``run`` is the only way in
        raise TypeError("a limited runner starts work only through run()")

    def launch_for(self, spec: RunSpec) -> Launch | None:
        return self.inner.launch_for(spec)

    def leaves(self, spec: RunSpec) -> Iterator[tuple[Runner, RunSpec]]:
        yield from self.inner.leaves(spec)
