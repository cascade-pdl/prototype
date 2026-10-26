import os
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


# --- wrapped: the same hooks as a container, on this host ---------------------

from cascade.node.node import DONE_MARKER  # noqa: E402
from cascade.protocol.binding import OutputDecl, OutputDecls  # noqa: E402
from cascade.store.file_store import FileConfig, FileStore  # noqa: E402
from cascade.types import TypeExpr  # noqa: E402


def _spec(tmp_path):
    store = FileStore(FileConfig(root=str(tmp_path / "store"), scope=("r1", "n")))
    return store, RunSpec(
        name="n", run_id="r1", instance_id="r1/n", store_out=store,
        outputs=OutputDecls((OutputDecl("o", TypeExpr.parse("int")),)),
    )


def _tool(tmp_path, exit_code=0):
    """Writes 7 to its output port and records where its staging root was."""
    seen = tmp_path / "root-was"
    return [sys.executable, "-c",
            "import os, pathlib; r = pathlib.Path(os.environ['CASCADE_ROOT']); "
            f"pathlib.Path({str(seen)!r}).write_text(str(r)); "
            "(r / 'outputs' / 'o').write_text('7'); "
            f"raise SystemExit({exit_code})"], seen


@pytest.mark.asyncio
async def test_a_wrapped_command_reads_and_writes_files_under_its_own_root(tmp_path):
    store, spec = _spec(tmp_path)
    cmd, seen = _tool(tmp_path)
    assert await RunnerSubprocess(cmd=cmd, cwd=str(tmp_path), wrap=True).run(spec) == 0
    assert store.get_json("o") == 7
    assert store.has(DONE_MARKER)
    assert not os.path.exists(seen.read_text())  # the staging root is removed afterwards


@pytest.mark.asyncio
async def test_a_failed_wrapped_command_marks_nothing_and_still_tidies_up(tmp_path):
    store, spec = _spec(tmp_path)
    cmd, seen = _tool(tmp_path, exit_code=4)
    assert await RunnerSubprocess(cmd=cmd, cwd=str(tmp_path), wrap=True).run(spec) == 4
    assert not store.has(DONE_MARKER)
    assert not os.path.exists(seen.read_text())


from cascade.protocol.run_spec import SPEC_BUDGET, SpecTooLarge  # noqa: E402


@pytest.mark.asyncio
async def test_a_launch_over_the_budget_is_refused_before_starting(tmp_path):
    """A host process is held to a container's budget: the ref may move into an image."""
    marker = tmp_path / "ran"
    spec = RunSpec(name="n", run_id="r", instance_id="r/n", args={"blob": "x" * SPEC_BUDGET})
    runner = RunnerSubprocess(cmd=[sys.executable, "-c", f"open({str(marker)!r}, 'w')"])
    with pytest.raises(SpecTooLarge, match="^r/n: "):
        await runner.spawn(spec)
    assert not marker.exists()
