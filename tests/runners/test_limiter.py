"""The run's concurrency limiter: slots taken by leaves, all pools at once."""
import asyncio

import pytest

from cascade.protocol.handle import Handle
from cascade.protocol.run_spec import Launch, RunSpec
from cascade.protocol.run_status import RunStatus
from cascade.protocol.runner import Runner
from cascade.runners.limiter import (
    LimitedRunner,
    Limiter,
    LimiterError,
    SlotRequest,
    backend_pool,
    named_pool,
)


A, B = backend_pool("a"), named_pool("b")


async def _settle() -> None:
    """Let every ready task run until it blocks again."""
    for _ in range(5):
        await asyncio.sleep(0)


async def _hold(limiter: Limiter, *pools, until: asyncio.Event, log: list | None = None, who=""):
    async with limiter.slot(SlotRequest(pools, who=who)):
        if log is not None:
            log.append(who)
        await until.wait()


@pytest.mark.asyncio
async def test_a_pool_caps_how_many_hold_a_slot():
    limiter, done = Limiter({A: 2}), asyncio.Event()
    tasks = [asyncio.create_task(_hold(limiter, A, until=done)) for _ in range(5)]
    await _settle()
    assert limiter.in_use(A) == 2
    done.set()
    await asyncio.gather(*tasks)
    assert limiter.in_use(A) == 0


@pytest.mark.asyncio
async def test_a_leaf_holds_no_pool_while_waiting_for_another():
    """All pools in one step: waiting for `b` must not tie up `a`, or two pools could
    deadlock each other and `a` would sit idle."""
    limiter = Limiter({A: 1, B: 1})
    release_b, release_rest = asyncio.Event(), asyncio.Event()
    log: list[str] = []
    holder = asyncio.create_task(_hold(limiter, B, until=release_b))
    await _settle()
    both = asyncio.create_task(_hold(limiter, A, B, until=release_rest, log=log, who="both"))
    await _settle()
    assert limiter.in_use(A) == 0  # "both" waits for b holding nothing
    only_a = asyncio.create_task(_hold(limiter, A, until=release_rest, log=log, who="only_a"))
    await _settle()
    assert log == ["only_a"]
    release_rest.set()
    await only_a
    release_b.set()
    await asyncio.gather(holder, both)
    assert log == ["only_a", "both"]
    assert limiter.in_use(A) == limiter.in_use(B) == 0


@pytest.mark.asyncio
async def test_waiters_are_served_in_arrival_order():
    limiter, release = Limiter({A: 1}), [asyncio.Event() for _ in range(4)]
    log: list[str] = []
    tasks = []
    for i in range(4):
        tasks.append(asyncio.create_task(_hold(limiter, A, until=release[i], log=log, who=str(i))))
        await _settle()
    for event in release:
        event.set()
        await _settle()
    await asyncio.gather(*tasks)
    assert log == ["0", "1", "2", "3"]


@pytest.mark.asyncio
async def test_a_leaf_cancelled_while_waiting_takes_nothing():
    limiter, release = Limiter({A: 1}), asyncio.Event()
    log: list[str] = []
    holder = asyncio.create_task(_hold(limiter, A, until=release))
    await _settle()
    cancelled = asyncio.create_task(_hold(limiter, A, until=release, log=log, who="cancelled"))
    later = asyncio.create_task(_hold(limiter, A, until=release, log=log, who="later"))
    await _settle()
    cancelled.cancel()
    await _settle()
    release.set()
    await asyncio.gather(holder, later)
    assert cancelled.cancelled()
    assert log == ["later"]
    assert limiter.in_use(A) == 0


@pytest.mark.asyncio
async def test_a_slot_granted_to_a_leaf_cancelled_before_it_resumes_is_given_back():
    limiter = Limiter({A: 1})
    holding = limiter.slot(SlotRequest((A,)))
    await holding.__aenter__()
    waiter = asyncio.create_task(_hold(limiter, A, until=asyncio.Event()))
    await _settle()
    await holding.__aexit__(None, None, None)  # grants the waiter's slot, without yielding
    assert limiter.in_use(A) == 1
    waiter.cancel()  # before the waiter has run again
    await _settle()
    assert waiter.cancelled()
    assert limiter.in_use(A) == 0


@pytest.mark.asyncio
async def test_a_failure_inside_the_slot_releases_it():
    limiter = Limiter({A: 1})
    with pytest.raises(RuntimeError):
        async with limiter.slot(SlotRequest((A,))):
            raise RuntimeError("boom")
    assert limiter.in_use(A) == 0


@pytest.mark.asyncio
async def test_an_unknown_pool_is_an_error_not_unlimited():
    with pytest.raises(LimiterError, match="limit 'b'"):
        async with Limiter({A: 1}).slot(SlotRequest((A, B), who="x")):
            pass


def test_capacities_are_positive():
    with pytest.raises(LimiterError):
        Limiter({A: 0})


# --- LimitedRunner -------------------------------------------------------------

class _Handle(Handle):
    def __init__(self, limiter: Limiter, seen: dict, finish: asyncio.Event):
        self.limiter, self.seen, self.finish = limiter, seen, finish

    async def state(self) -> RunStatus:
        return RunStatus(running=False, exit_code=0)

    async def await_done(self) -> RunStatus:
        await self.finish.wait()
        return RunStatus(running=False, exit_code=0)

    async def kill(self) -> RunStatus:
        self.seen["in_use_at_kill"] = self.limiter.in_use(A)
        return RunStatus(running=False)


class _Leaf(Runner):
    def __init__(self, limiter: Limiter, finish: asyncio.Event):
        self.limiter, self.finish, self.seen = limiter, finish, {}

    async def spawn(self, spec: RunSpec) -> Handle:
        self.seen["in_use_at_spawn"] = self.limiter.in_use(A)
        return _Handle(self.limiter, self.seen, self.finish)

    def launch_for(self, spec: RunSpec) -> Launch:
        return Launch(env={"K": "v"}, command=[])


def _spec() -> RunSpec:
    return RunSpec(name="r", run_id="run", instance_id="run/main/n")


@pytest.mark.asyncio
async def test_a_limited_runner_holds_its_slot_from_spawn_until_the_work_stops():
    limiter, finish = Limiter({A: 1}), asyncio.Event()
    leaf = _Leaf(limiter, finish)
    running = asyncio.create_task(LimitedRunner(leaf, limiter, (A,)).run(_spec()))
    await _settle()
    assert leaf.seen["in_use_at_spawn"] == 1
    finish.set()
    assert await running == 0
    assert limiter.in_use(A) == 0


@pytest.mark.asyncio
async def test_a_cancelled_limited_runner_kills_its_work_before_freeing_the_slot():
    limiter = Limiter({A: 1})
    leaf = _Leaf(limiter, asyncio.Event())
    running = asyncio.create_task(LimitedRunner(leaf, limiter, (A,)).run(_spec()))
    await _settle()
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running
    assert leaf.seen["in_use_at_kill"] == 1
    assert limiter.in_use(A) == 0


@pytest.mark.asyncio
async def test_a_limited_runner_cannot_be_spawned_around_its_slot():
    limiter = Limiter({A: 1})
    with pytest.raises(TypeError):
        await LimitedRunner(_Leaf(limiter, asyncio.Event()), limiter, (A,)).spawn(_spec())


def test_a_limited_runner_is_checked_as_its_inner_runner():
    limiter = Limiter({A: 1})
    leaf = _Leaf(limiter, asyncio.Event())
    limited = LimitedRunner(leaf, limiter, (A,))
    assert [r for r, _ in limited.leaves(_spec())] == [leaf]
    assert limited.launch_for(_spec()) == leaf.launch_for(_spec())
