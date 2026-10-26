"""``cascade.runners.registry`` — a RunConfig on a Backend becomes a live Runner.

Two inputs that say different things, so nothing is merged: the ref says what runs and
how big it is, the backend says where and with which machine settings.
"""
import pytest

from cascade.model.runner_kinds import RunnerKind
from cascade.model.ref_data import RefDocker, RefEcho, RefSubprocess
from cascade.plan.run_config import RunConfig
from cascade.model.resources import Resources
from cascade.protocol.run_spec import RunSpec
from cascade.plan.signature import Port, Signature, TypeExpr
from cascade.store.file_store import FileStore, FileConfig

from cascade.runners.registry import BundleError, RunnerKindError, build_runner, RUNNER_BUILDERS
from cascade.substrate.backends import Backend, BackendKind, EcsConfig, LocalConfig, RUNS
from cascade.runners.docker import RunnerDocker
from cascade.runners.echo import RunnerEcho
from cascade.runners.process import RunnerSubprocess


def _local(**config) -> Backend:
    return Backend(name="here", kind=BackendKind.local, max=Resources(8, 16384),
                   config=LocalConfig(**config))


def test_every_placeable_pairing_has_a_builder():
    """A registry that cannot cover what placement allows is incomplete — this is what
    was missing while RunnerKind.echo had no runner."""
    assert set(RUNNER_BUILDERS) == {(b, r) for b, kinds in RUNS.items() for r in kinds}
    assert {r for _b, r in RUNNER_BUILDERS} | {RunnerKind.echo} == set(RunnerKind)


# --- dispatch ----------------------------------------------------------------

def test_builds_a_docker_runner():
    config = RunConfig(
        config=RefDocker(image="flat-bug:v3", extra_args=["--gpus", "all"], wrap=False),
    )
    runner = build_runner(config)
    assert isinstance(runner, RunnerDocker)
    assert runner.image == "flat-bug:v3"
    assert runner.extra_args == ["--gpus", "all"]


def test_builds_a_subprocess_runner():
    config = RunConfig(config=RefSubprocess(cmd=["ls", "-l"]))
    runner = build_runner(config)
    assert isinstance(runner, RunnerSubprocess)
    assert runner.cmd == ["ls", "-l"]


def test_builds_an_echo_runner():
    config = RunConfig(config=RefEcho(message="hi"))
    runner = build_runner(config)
    assert isinstance(runner, RunnerEcho)
    assert runner.message == "hi"


# --- backend settings ----------------------------------------------------------

def test_docker_no_pull_defaults_to_true():
    runner = build_runner(RunConfig(config=RefDocker(image="x:1", wrap=False)))
    assert runner.no_pull is True


def test_the_backend_supplies_the_machine_settings():
    runner = build_runner(
        RunConfig(config=RefDocker(image="x:1", wrap=False)),
        _local(no_pull=False, map_current_user=False),
    )
    assert (runner.no_pull, runner.map_current_user) == (False, False)


def test_a_local_docker_runner_joins_the_container_network():
    from cascade.runners.docker import CONTAINER_NETWORK

    runner = build_runner(RunConfig(config=RefDocker(image="x:1", wrap=False)), _local())
    assert runner.network == CONTAINER_NETWORK


def test_ecs_is_placeable_but_not_built_yet():
    ecs = Backend(name="ecs-cpu", kind=BackendKind.ecs, max=Resources(16, 122880),
                  config=EcsConfig(cluster="c", subnets=("s",)), concurrency=4)
    with pytest.raises(RunnerKindError, match="not built yet"):
        build_runner(RunConfig(config=RefDocker(image="x:1", wrap=False)), ecs)


def test_ecs_cannot_run_a_subprocess():
    ecs = Backend(name="ecs-cpu", kind=BackendKind.ecs, max=Resources(16, 122880),
                  config=EcsConfig(cluster="c", subnets=("s",)), concurrency=4)
    with pytest.raises(RunnerKindError, match="cannot run a subprocess"):
        build_runner(RunConfig(config=RefSubprocess(cmd=["x"])), ecs)


# --- resources ------------------------------------------------------------------

def test_resource_limits_reach_the_docker_command():
    config = RunConfig(
        config=RefDocker(image="x:1", wrap=False),
        resources=Resources(cpu=2, memory=2048, gpu=1),
    )
    cmd = build_runner(config)._build_cmd(RunSpec(name="n", run_id="r"))
    assert "--memory" in cmd and "2048m" in cmd
    assert "--gpus" in cmd and "1" in cmd
    assert "--cpus" in cmd and "2" in cmd

# --- echo's output ports come from the signature -----------------------------

def test_echo_takes_its_output_ports_from_the_signature():
    """Per-runnable, like the runner itself — nothing here varies per instance."""
    sig = Signature(
        inputs={"d": Port(TypeExpr.parse("Detection"))},
        outputs={"scored": Port(TypeExpr.parse("Score")), "log": Port(TypeExpr.parse("string"))},
    )
    runner = build_runner(
        RunConfig(config=RefEcho(message="hi")),
        signature=sig,
    )
    assert runner.outputs == {"scored": 0, "log": 0}


def test_echo_without_a_signature_writes_nothing():
    runner = build_runner(RunConfig(config=RefEcho()))
    assert runner.outputs == {}


def test_other_kinds_ignore_the_signature():
    sig = Signature(inputs={}, outputs={"o": Port(TypeExpr.parse("string"))})
    runner = build_runner(
        RunConfig(config=RefDocker(image="x:1", wrap=False)),
        signature=sig,
    )
    assert isinstance(runner, RunnerDocker)


@pytest.mark.asyncio
async def test_echo_writes_a_stub_for_each_output_port(tmp_path):
    """What makes a multi-node dag testable: a downstream node has something to read.
    Where the stubs land is decided by store_out's scope, not by echo."""
    store = FileStore(FileConfig(root=str(tmp_path), scope=("r1", "main", "d")))
    runner = build_runner(
        RunConfig(config=RefEcho(message="detected")),
        signature=Signature(inputs={}, outputs={"dets": Port(TypeExpr.parse("Detection[]"))}),
    )
    rc = await runner.run(RunSpec(name="detect", run_id="r1", instance_id="r1/main/d", store_out=store))
    assert rc == 0
    written = store.get_json("dets")
    # Detection[] is depth 1, so the stub is a list -- a downstream node can fan it
    assert isinstance(written, list)
    assert written[0]["port"] == "dets"
    assert written[0]["echo"] == "detected"
    assert (tmp_path / "r1" / "main" / "d" / "dets").is_file()


@pytest.mark.asyncio
async def test_echo_is_harmless_without_a_writer_store():
    runner = build_runner(
        RunConfig(config=RefEcho()),
        signature=Signature(inputs={}, outputs={"o": Port(TypeExpr.parse("string"))}),
    )
    assert await runner.run(RunSpec(name="n", run_id="r")) == 0

# --- docker must mount a file store -------------------------------------------

def _docker_for(spec_stores=True, tmp_path=None):
    from cascade.protocol.run_spec import RunSpec
    from cascade.store.file_store import FileConfig, FileStore

    runner = build_runner(RunConfig(config=RefDocker(image="x:1", wrap=False)))
    if not spec_stores:
        return runner, RunSpec(name="n", run_id="r")
    base = FileConfig(root=str(tmp_path / "_store"), scope=("r1", "main"))
    spec = RunSpec(
        name="n", run_id="r1", instance_id="r1/main/n",
        store_in=FileStore(base), store_out=FileStore(base.subscope(("n",))),
    )
    return runner, spec


def test_a_file_store_root_is_mounted_into_the_container(tmp_path):
    """Without this the container writes into its own filesystem and the data is
    discarded on exit — the run leaves only the plan the executor wrote host-side."""
    from cascade.runners.docker import CONTAINER_STORE_ROOT

    runner, spec = _docker_for(tmp_path=tmp_path)
    cmd = runner._build_cmd(spec)
    root = str((tmp_path / "_store").resolve())
    assert "-v" in cmd
    assert f"{root}:{CONTAINER_STORE_ROOT}" in cmd


def test_the_container_side_is_a_posix_path_not_the_host_path(tmp_path):
    """A host path is not a valid mount target for a Linux container: `-v C:\\x:C:\\x`
    is meaningless on Windows. So the container side is a fixed POSIX path, and the store
    root in the spec is rewritten to match."""
    from cascade.runners.docker import CONTAINER_STORE_ROOT

    runner, spec = _docker_for(tmp_path=tmp_path)
    mount = [c for c in runner._build_cmd(spec) if c.endswith(CONTAINER_STORE_ROOT)][0]
    assert mount.endswith(f":{CONTAINER_STORE_ROOT}")
    assert CONTAINER_STORE_ROOT.startswith("/")


def test_the_container_sees_the_rewritten_root(tmp_path):
    """The scope is untouched, so addressing still resolves — only the root changes."""
    import json

    from cascade.runners.docker import CONTAINER_STORE_ROOT
    from cascade.store.registry import decode as decode_store

    runner, spec = _docker_for(tmp_path=tmp_path)
    cmd = runner._build_cmd(spec)
    blob = [c.split("=", 1)[1] for c in cmd if c.startswith("CASCADE_STORE_OUT=")][0]
    store = decode_store(json.loads(blob))
    assert store.config.root == CONTAINER_STORE_ROOT
    assert store.config.scope == ("r1", "main", "n")  # unchanged


def test_one_mount_even_though_there_are_two_stores(tmp_path):
    """Reader and writer are subscopes of one deployment store, so they share a root."""
    runner, spec = _docker_for(tmp_path=tmp_path)
    assert sum(1 for c in runner._build_cmd(spec) if c == "-v") == 1


def test_no_mount_when_there_is_no_file_store():
    runner, spec = _docker_for(spec_stores=False)
    assert "-v" not in runner._build_cmd(spec)


def test_home_is_never_forced(tmp_path):
    """Forcing HOME breaks any image that pip-installed as its own user: the packages sit
    in that user's ~/.local, and a different HOME hides them."""
    runner, spec = _docker_for(tmp_path=tmp_path)
    assert not [c for c in runner._build_cmd(spec) if c.startswith("HOME=")]

# --- wrapping: the bundle path and the opt-out --------------------------------

def _bundle(at):
    (at / "bin").mkdir(parents=True)
    (at / "bin" / "cascade").write_text("#!/opt/cascade/bin/python3 -I\n")
    return at


def test_a_wrapped_docker_ref_gets_the_bundle_relative_to_the_pipeline(tmp_path):
    _bundle(tmp_path / "dist" / "opt-cascade")
    runner = build_runner(
        RunConfig(config=RefDocker(image="x:1")), _local(bundle="dist/opt-cascade"),
        cwd=str(tmp_path),
    )
    assert runner.bundle == str(tmp_path / "dist" / "opt-cascade")


def test_a_bundle_path_that_is_not_a_bundle_is_named(tmp_path):
    with pytest.raises(BundleError, match="no cascade bundle at .*/nowhere"):
        build_runner(RunConfig(config=RefDocker(image="x:1")), _local(bundle="nowhere"),
                     cwd=str(tmp_path))


def test_a_wrapped_docker_ref_on_a_backend_without_a_bundle_is_refused():
    with pytest.raises(BundleError, match="has no `bundle`"):
        build_runner(RunConfig(config=RefDocker(image="x:1")), _local())


def test_an_unwrapped_docker_ref_needs_no_bundle():
    assert build_runner(RunConfig(config=RefDocker(image="x:1", wrap=False))).bundle is None


def test_the_command_reaches_the_docker_runner():
    runner = build_runner(RunConfig(config=RefDocker(image="x:1", command=["a"], wrap=False)))
    assert runner.command == ["a"]


def test_a_subprocess_ref_says_whether_it_is_wrapped():
    assert build_runner(RunConfig(config=RefSubprocess(cmd=["t"]))).wrap is True
    assert build_runner(RunConfig(config=RefSubprocess(cmd=["t"], wrap=False))).wrap is False


def test_a_built_ref_runs_the_image_named_by_its_hash_and_never_pulls_it():
    ref = RefDocker(build="refs/numbers/", build_hash="0123456789abcdef" * 4, wrap=False)
    runner = build_runner(RunConfig(config=ref), _local(no_pull=False))
    assert runner.image == "cascade/numbers:0123456789abcdef"
    assert runner.no_pull is True  # built here, before the run: nowhere to pull from


def test_a_built_ref_without_its_hash_was_not_compiled():
    with pytest.raises(ValueError, match="no build hash"):
        build_runner(RunConfig(config=RefDocker(build="refs/x/", wrap=False)))
