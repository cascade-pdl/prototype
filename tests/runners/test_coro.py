import pytest

from cascade.runners.coro import RunnerCoro, RunnerAwaitable
from cascade.protocol.run_spec import RunSpec


SPEC = RunSpec(name="testme", run_id="testid", instance_id="main/n")


@pytest.mark.asyncio
async def test_runner_awaitable():

    async def testme():
        import asyncio

        await asyncio.sleep(0.1)
        return "done!"

    runner = RunnerAwaitable(coro=testme)
    handle = await runner.spawn(
        spec=RunSpec(
            name="testme",
            run_id="testid",
        ),
    )
    assert (await handle.await_done()).exit_code == "done!"


@pytest.mark.asyncio
async def test_awaitable_passes_bound_args():
    async def add(a, b):
        return a + b

    handle = await RunnerAwaitable(coro=add, args=(2,), kwas={"b": 3}).spawn(spec=SPEC)
    assert (await handle.await_done()).exit_code == 5


@pytest.mark.asyncio
async def test_coro_receives_the_spec():
    """The point of the RunnerCoro/RunnerAwaitable split: this variant hands the
    callable its spec, which is what makes a local dag runner substitutable for a
    remote one. Every phase-2 runner is constructed through it."""
    seen = {}

    async def testme(spec: RunSpec):
        seen["run_id"] = spec.run_id
        seen["instance_id"] = spec.instance_id
        return 0

    handle = await RunnerCoro(coro=testme).spawn(spec=SPEC)
    assert (await handle.await_done()).exit_code == 0
    assert seen == {"run_id": "testid", "instance_id": "main/n"}


@pytest.mark.asyncio
async def test_run_treats_a_none_result_as_success():
    async def testme(spec):
        return None

    assert await RunnerCoro(coro=testme).run(SPEC) == 0


@pytest.mark.asyncio
async def test_exceptions_propagate_through_the_handle():
    async def boom(spec):
        raise RuntimeError("nope")

    handle = await RunnerCoro(coro=boom).spawn(spec=SPEC)
    with pytest.raises(RuntimeError):
        await handle.await_done()

# --- kill --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_kill_cancels_a_running_coroutine():
    import asyncio

    stopped = asyncio.Event()

    async def forever(spec):
        try:
            await asyncio.sleep(60)
        finally:
            stopped.set()

    handle = await RunnerCoro(coro=forever).spawn(spec=SPEC)
    await asyncio.sleep(0)  # let it start
    status = await handle.kill()
    assert stopped.is_set()
    assert status.running is False and status.exit_code is None


@pytest.mark.asyncio
async def test_kill_after_completion_reports_the_result_and_is_idempotent():
    async def five(spec):
        return 5

    handle = await RunnerCoro(coro=five).spawn(spec=SPEC)
    await handle.await_done()
    assert (await handle.kill()).exit_code == 5
    assert (await handle.kill()).exit_code == 5


@pytest.mark.asyncio
async def test_cancelling_run_cancels_the_work_it_spawned():
    """run() kills its handle when interrupted, so cancelling the waiter stops the work
    instead of merely abandoning it."""
    import asyncio

    stopped = asyncio.Event()

    async def forever(spec):
        try:
            await asyncio.sleep(60)
        finally:
            stopped.set()

    waiter = asyncio.create_task(RunnerCoro(coro=forever).run(SPEC))
    await asyncio.sleep(0.05)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert stopped.is_set()
