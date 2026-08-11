import pytest

from cascade.engine.runner.runner_coro import RunnerAwaitable
from cascade.engine.runner.run_spec import RunSpec


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
    res = await handle.await_done()
