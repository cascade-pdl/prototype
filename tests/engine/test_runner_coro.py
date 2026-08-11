import pytest

from cascade.engine.runner.runner_coro import RunnerCoro, RunnerAwaitable
from cascade.engine.run_spec import RunSpec


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