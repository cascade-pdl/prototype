"""Docker kill: the container is stopped by name, not just its ``docker run`` client.

No docker daemon is needed: the command line is inspected, and the client is a real
process standing in for ``docker run`` while ``docker stop`` is intercepted.
"""
import asyncio

import pytest

from cascade.protocol.run_spec import RunSpec
from cascade.runners.docker import HandleDocker, RunnerDocker, container_name
from tests.support.processes import alive, pid_from, posix_only, sleeper


def test_container_name_is_readable_valid_and_unique():
    spec = RunSpec(name="detect", run_id="r1", instance_id="r1/main/p/0/d")
    a, b = container_name(spec), container_name(spec)
    assert a.startswith("cascade.r1.main.p.0.d.")
    assert a != b  # a re-run with the same run id must not collide
    weird = container_name(RunSpec(name="x", run_id="r", instance_id="r/a b:c$"))
    assert weird.startswith("cascade.r.a_b_c_.")


def test_the_container_is_started_under_that_name():
    cmd = RunnerDocker(image="x:1")._build_cmd(RunSpec(name="n", run_id="r"), name="cascade.r.n.abc")
    assert cmd[cmd.index("--name") + 1] == "cascade.r.n.abc"
    assert cmd[-1] == "x:1"  # the image stays last


@pytest.mark.skipif(posix_only, reason="POSIX signals")
@pytest.mark.asyncio
async def test_kill_stops_the_container_by_name_then_the_client(tmp_path, monkeypatch):
    pidfile = tmp_path / "pid"
    client = await asyncio.create_subprocess_exec(*sleeper(pidfile))
    handle = HandleDocker(client, name="cascade.r.n.abc")
    pid = await pid_from(pidfile)

    stops = []
    real_exec = asyncio.create_subprocess_exec

    async def fake_exec(*argv, **kwargs):
        if argv[:2] == ("docker", "stop"):
            stops.append(argv)
            argv = ("true",)  # no daemon here; pretend the stop succeeded
        return await real_exec(*argv, **kwargs)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    await handle.kill()

    assert stops == [("docker", "stop", "--time", "5", "cascade.r.n.abc")]
    assert not alive(pid)  # and the client did not outlive it


@pytest.mark.asyncio
async def test_kill_after_the_container_exited_does_not_call_docker(monkeypatch):
    client = await asyncio.create_subprocess_exec("true")
    handle = HandleDocker(client, name="cascade.r.n.abc")
    await handle.await_done()

    async def no_exec(*argv, **kwargs):
        raise AssertionError(f"unexpected {argv}")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", no_exec)
    assert (await handle.kill()).exit_code == 0
