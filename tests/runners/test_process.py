import sys

import pytest

from cascade.runners.process import RunnerSubprocess
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

# --- kill --------------------------------------------------------------------

import asyncio

from tests.support.processes import alive, pid_from, posix_only, sleeper


@pytest.mark.skipif(posix_only, reason="POSIX signals")
@pytest.mark.asyncio
async def test_kill_terminates_the_process(tmp_path):
    pidfile = tmp_path / "pid"
    handle = await RunnerSubprocess(cmd=sleeper(pidfile)).spawn(RunSpec(name="n", run_id="r"))
    pid = await pid_from(pidfile)

    status = await handle.kill()
    assert status.running is False
    assert status.exit_code == -15  # SIGTERM was enough
    assert not alive(pid)
    assert (await handle.kill()).exit_code == -15  # idempotent


@pytest.mark.skipif(posix_only, reason="POSIX signals")
@pytest.mark.asyncio
async def test_kill_escalates_to_sigkill_after_the_grace_period(tmp_path):
    pidfile = tmp_path / "pid"
    handle = await RunnerSubprocess(cmd=sleeper(pidfile, ignore_term=True)).spawn(
        RunSpec(name="n", run_id="r")
    )
    handle.kill_grace = 0.2
    pid = await pid_from(pidfile)

    status = await handle.kill()
    assert status.exit_code == -9
    assert not alive(pid)


@pytest.mark.asyncio
async def test_kill_after_exit_just_reports_the_code():
    handle = await RunnerSubprocess(cmd=[sys.executable, "-c", "raise SystemExit(4)"]).spawn(
        RunSpec(name="n", run_id="r")
    )
    await handle.await_done()
    assert (await handle.kill()).exit_code == 4


@pytest.mark.skipif(posix_only, reason="POSIX signals")
@pytest.mark.asyncio
async def test_cancelling_run_kills_the_process(tmp_path):
    """The bug this fixes: cancelling the wait used to leave the process running."""
    pidfile = tmp_path / "pid"
    waiter = asyncio.create_task(
        RunnerSubprocess(cmd=sleeper(pidfile)).run(RunSpec(name="n", run_id="r"))
    )
    pid = await pid_from(pidfile)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert not alive(pid)
