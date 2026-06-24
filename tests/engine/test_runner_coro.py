import pytest

from cascade.engine.runner.runner_coro import RunnerCoro, HandleCoro
from cascade.engine.runner.run_spec import RunSpec


@pytest.mark.asyncio
async def test_runner_coro():

    async def testme():
        import asyncio

        await asyncio.sleep(0.1)
        return "done!"

    runner = RunnerCoro(coro=testme)
    handle = await runner.spawn(
        spec=RunSpec(
            name="testme",
            run_id="testid",
        ),
    )
    res = await handle.await_done()
