import sys

import pytest

from cascade.engine.runner.runner_subprocess import RunnerSubprocess
from cascade.protocol.run_spec import RunSpec


@pytest.mark.asyncio
async def test_runner_coro():
    runner = RunnerSubprocess(cmd=[sys.executable, "-c", "print('hi')"])
    handle = await runner.spawn(
        spec=RunSpec(
            name="testme",
            run_id="testid",
        ),
    )
    assert (await handle.await_done()).exit_code == 0