import pytest

from cascade.engine.runner.runner_subprocess import RunnerSubprocess, HandleSubprocess
from cascade.engine.runner.run_spec import RunSpec


@pytest.mark.asyncio
async def test_runner_coro():
    runner = RunnerSubprocess(cmd=["time", "sleep", "0.1"])
    handle = await runner.spawn(
        spec=RunSpec(
            name="testme",
            run_id="testid",
        ),
    )
    res = await handle.await_done()
