"""Docker kill: the container is stopped by name, not just its ``docker run`` client.

No docker daemon is needed: the command line is inspected, and the client is a real
process standing in for ``docker run`` while ``docker stop`` is intercepted.
"""
import asyncio
import json

import pytest

import os

from cascade.protocol.run_spec import SPEC_BUDGET, RunSpec, SpecTooLarge
from cascade.store.file_store import FileConfig, FileStore
from cascade.runners.credentials import ContainerCredentials
from cascade.runners.docker import (
    CONTAINER_NETWORK,
    DockerImageError,
    HandleDocker,
    ImageConfig,
    RunnerDocker,
    container_name,
    inspect_image,
)
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


# --- wrapped: the bundle as entrypoint, the image's own command after `--` ----

SPEC = RunSpec(name="n", run_id="r")
IMAGE = ImageConfig(id="sha256:abc", entrypoint=("/usr/bin/tool",), cmd=("--default",))


def wrapped() -> list[str]:
    return RunnerDocker(image="tool:1", bundle="/b/opt-cascade")._build_cmd(
        SPEC, name="c", image=IMAGE)


def test_a_wrapped_run_makes_the_bundle_the_entrypoint():
    cmd = wrapped()
    assert cmd[cmd.index("--entrypoint") + 1] == "/opt/cascade/bin/cascade"
    assert "/b/opt-cascade:/opt/cascade:ro" in cmd       # mounted read-only
    assert "/cascade:mode=1777" in cmd                     # staging writable by any user


def test_the_image_runs_by_id_then_its_own_entrypoint_and_command_follow_wrap():
    """By id: every container of the run is exactly the image that was inspected."""
    cmd = wrapped()
    after = cmd[cmd.index("sha256:abc") + 1:]
    assert after == ["node", "wrap", "--", "/usr/bin/tool", "--default"]
    assert "tool:1" not in cmd


def test_command_replaces_the_default_command_and_keeps_the_entrypoint():
    cmd = RunnerDocker(image="tool:1", bundle="/b", command=["-x", "in"])._build_cmd(
        SPEC, name="c", image=IMAGE)
    assert cmd[-5:] == ["wrap", "--", "/usr/bin/tool", "-x", "in"]


def test_an_image_with_nothing_to_run_is_an_error():
    bare = ImageConfig(id="sha256:abc", entrypoint=(), cmd=())
    with pytest.raises(DockerImageError, match="nothing to run"):
        RunnerDocker(image="scratch:1", bundle="/b")._build_cmd(SPEC, name="c", image=bare)


def test_extra_args_still_come_before_the_image():
    cmd = RunnerDocker(image="t:1", bundle="/b", extra_args=["--shm-size", "1g"])._build_cmd(
        SPEC, name="c", image=IMAGE)
    assert cmd.index("--shm-size") < cmd.index("sha256:abc")


def test_unwrapped_runs_the_image_as_published_with_any_command_after_it():
    cmd = RunnerDocker(image="mine:1", command=["serve", "--once"])._build_cmd(SPEC, name="c")
    assert cmd[-3:] == ["mine:1", "serve", "--once"]
    assert "--entrypoint" not in cmd


# --- inspecting the image, against a stand-in `docker` ------------------------

FAKE_DOCKER = """#!/bin/sh
case "$1 $2" in
  "image inspect")
    if [ -f "$FAKE_DOCKER_STATE/present" ]; then
      echo '[{"Id": "sha256:abc", "Config": {"Entrypoint": ["/e"], "Cmd": null}}]'
    else
      echo "Error: No such image: $3" >&2; exit 1
    fi ;;
  "pull --quiet") echo "$3" >> "$FAKE_DOCKER_STATE/pulled"; touch "$FAKE_DOCKER_STATE/present" ;;
  *) exit 2 ;;
esac
"""


@pytest.fixture
def fake_docker(tmp_path, monkeypatch):
    bin_dir, state = tmp_path / "bin", tmp_path / "state"
    bin_dir.mkdir()
    state.mkdir()
    (bin_dir / "docker").write_text(FAKE_DOCKER)
    (bin_dir / "docker").chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_DOCKER_STATE", str(state))
    return state


@pytest.mark.skipif(posix_only, reason="a shell script stands in for docker")
@pytest.mark.asyncio
async def test_inspect_reads_id_entrypoint_and_command(fake_docker):
    (fake_docker / "present").touch()
    assert await inspect_image("t:1", pull=False) == ImageConfig("sha256:abc", ("/e",), ())


@pytest.mark.skipif(posix_only, reason="a shell script stands in for docker")
@pytest.mark.asyncio
async def test_a_missing_image_is_not_pulled_when_the_backend_says_no_pull(fake_docker):
    with pytest.raises(DockerImageError, match="`no_pull`"):
        await inspect_image("t:1", pull=False)
    assert not (fake_docker / "pulled").exists()


@pytest.mark.skipif(posix_only, reason="a shell script stands in for docker")
@pytest.mark.asyncio
async def test_a_missing_image_is_pulled_when_allowed(fake_docker):
    config = await inspect_image("t:1", pull=True)
    assert (fake_docker / "pulled").read_text().split() == ["t:1"]
    assert config.id == "sha256:abc"


# --- an image resolved once for the run --------------------------------------

@pytest.mark.asyncio
async def test_a_resolved_image_means_no_docker_call_before_the_run(monkeypatch):
    """A fan of a thousand lanes must not inspect the image a thousand times."""
    import cascade.runners.docker as docker_module

    async def no_inspect(*a, **k):
        raise AssertionError("inspected per spawn")

    started = []
    real_exec = asyncio.create_subprocess_exec

    async def record(*argv, **kwargs):
        started.append(argv)
        return await real_exec("true")

    monkeypatch.setattr(docker_module, "inspect_image", no_inspect)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", record)
    handle = await RunnerDocker(image="tool:1", bundle="/b", resolved=IMAGE).spawn(SPEC)
    await handle.await_done()
    [argv] = started
    assert "sha256:abc" in argv and "tool:1" not in argv


def test_an_unwrapped_resolved_image_runs_by_id_too():
    cmd = RunnerDocker(image="mine:1", resolved=IMAGE)._build_cmd(SPEC, name="c", image=IMAGE)
    assert cmd[-1] == "sha256:abc"


# --- the spec budget -----------------------------------------------------------

def test_the_launch_is_exactly_what_the_command_line_passes(tmp_path):
    """The budget is checked on ``launch_for``, so it must be what ``docker run`` gets:
    the container's env (store rerooted) and the command after the image. Credentials
    are passed besides, by name: on ECS the agent sets its own."""
    store = FileStore(FileConfig(root=str(tmp_path / "s"), scope=("r", "n")))
    spec = RunSpec(name="n", run_id="r", store_out=store, args={"k": 1})
    runner = RunnerDocker(image="tool:1", bundle="/b", resolved=IMAGE, credentials=CREDENTIALS)
    launch = runner.launch_for(spec)
    cmd = runner._build_cmd(spec, name="c", image=IMAGE)

    passed = [cmd[i + 1] for i, a in enumerate(cmd) if a == "-e"]
    assert passed == [f"{k}={v}" for k, v in launch.env.items()] + list(CREDENTIALS.env())
    assert '"/cascade/store"' in launch.env["CASCADE_STORE_OUT"]
    assert not set(CREDENTIALS.env()) & set(launch.env)
    assert cmd[cmd.index("sha256:abc") + 1:] == list(launch.command)


# --- the network, credentials, and a store on the host ---------------------------

CREDENTIALS = ContainerCredentials(uri="http://169.254.170.2:1/credentials", token="secret")


def test_the_token_is_never_on_the_command_line():
    cmd = RunnerDocker(image="t:1", credentials=CREDENTIALS)._build_cmd(SPEC)
    assert not [a for a in cmd if "secret" in a]


@pytest.mark.asyncio
async def test_the_credentials_reach_the_docker_client_environment(monkeypatch):
    seen = {}
    real_exec = asyncio.create_subprocess_exec

    async def record(*argv, env=None, **kwargs):
        seen.update(env or {})
        return await real_exec("true")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", record)
    handle = await RunnerDocker(image="t:1", credentials=CREDENTIALS).spawn(SPEC)
    await handle.await_done()
    assert seen["AWS_CONTAINER_AUTHORIZATION_TOKEN"] == "secret"
    assert seen["PATH"] == os.environ["PATH"]  # the client's own environment kept


def test_the_container_joins_the_network():
    cmd = RunnerDocker(image="t:1", network=CONTAINER_NETWORK)._build_cmd(SPEC)
    assert cmd[cmd.index("--network") + 1] == "cascade"


def _s3_spec(endpoint):
    from cascade.store.s3_store import S3Config, S3Store

    config = S3Config(bucket="b", region="garage", endpoint=endpoint, scope=("r",))
    return RunSpec(name="n", run_id="r", store_in=S3Store(config), store_out=S3Store(config))


@pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "127.1.2.3", "[::1]"])
def test_a_store_on_the_host_loopback_is_reached_at_the_gateway(host):
    runner = RunnerDocker(image="t:1", network=CONTAINER_NETWORK)
    env = runner.launch_for(_s3_spec(f"http://{host}:3900/x")).env
    for key in ("CASCADE_STORE_IN", "CASCADE_STORE_OUT"):
        assert json.loads(env[key])["config"]["endpoint"] == "http://169.254.170.2:3900/x"


@pytest.mark.parametrize("endpoint", [None, "https://s3.example.org", "http://10.0.0.5:3900"])
def test_other_endpoints_are_left_alone(endpoint):
    env = RunnerDocker(image="t:1", network=CONTAINER_NETWORK).launch_for(_s3_spec(endpoint)).env
    assert json.loads(env["CASCADE_STORE_OUT"])["config"]["endpoint"] == endpoint


def test_a_wrapped_launch_is_unknown_until_the_image_is_resolved():
    assert RunnerDocker(image="tool:1", bundle="/b").launch_for(SPEC) is None


def test_a_launch_over_the_budget_is_refused():
    spec = RunSpec(name="n", run_id="r", instance_id="r/main/n", args={"blob": "x" * SPEC_BUDGET})
    with pytest.raises(SpecTooLarge, match="^r/main/n: .*CASCADE_ARGS"):
        RunnerDocker(image="tool:1")._build_cmd(spec, name="c")
