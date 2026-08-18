"""The mock pipeline in `examples/mock/`, as real subprocesses against a real store.

This is the integration test the milestone tests are not: actual OS processes, actual
files, the node-side contract exercised for real rather than through `RunnerEcho`. It
is also the honest record of where the executor currently stops — the scattered variant
asserts the *designed* failure rather than pretending to pass.
"""
import sys
from pathlib import Path

import pytest
from yaml import safe_load

from cascade.model.pipeline import Pipeline
from cascade.plan.compile import check, compile_pipeline
from cascade.engine.binding import InputBindings
from cascade.engine.run_spec import RunSpec
from cascade.engine.runner.runner_dag import DagRunner
from cascade.store.file_store import FileConfig, FileStore


def _repo_root() -> Path:
    """Walk up to the directory holding pyproject.toml.

    Not a fixed number of ``parents``: this test is equally at home in ``tests/`` or
    ``tests/engine/``, and hard-coding the depth silently resolves *above* the repo when
    it moves — which fails with a path that looks plausible.
    """
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").is_file():
            return parent
    raise RuntimeError("no pyproject.toml above this test; cannot locate the repo root")


ROOT = _repo_root()
EXAMPLES = ROOT / "examples" / "mock"
FLAT = EXAMPLES / "pipeline_flat.yaml"
SCATTERED = EXAMPLES / "pipeline.yaml"


def _plan(path: Path):
    pipeline = Pipeline.decode(safe_load(path.read_text()))
    assert check(pipeline) == []
    return compile_pipeline(pipeline)


def _with_this_interpreter(plan):
    """Rewrite each ref's command for the test environment.

    The pipelines say `python` and name their scripts *project-relative*, which is what
    lets `cascade run` work from inside `examples/mock`. Neither holds here: the tests may
    run under a venv interpreter that `python` does not resolve to, and from any cwd — so
    both halves are made absolute.
    """
    for config in plan.run_config.values():
        cmd = list(config.config.cmd)
        config.config.cmd = [sys.executable, str(EXAMPLES / cmd[-1])]
    return plan


async def _run(plan, tmp_path, run_id="r1"):
    dag = plan.entrypoint
    store = FileStore(FileConfig(root=str(tmp_path), scope=("mock",)).subscope((run_id, dag)))
    runner = DagRunner(dag, plan)
    code = await runner.run(
        RunSpec(
            name=dag, run_id=run_id, instance_id=f"{run_id}/{dag}",
            store_out=store, inputs=InputBindings(),
        )
    )
    return code, runner, store


def test_both_mock_pipelines_compile():
    assert _plan(FLAT).entrypoint == "main"
    assert _plan(SCATTERED).entrypoint == "main"


@pytest.mark.asyncio
async def test_flat_pipeline_runs_as_real_subprocesses(tmp_path):
    plan = _with_this_interpreter(_plan(FLAT))
    code, runner, store = await _run(plan, tmp_path)
    assert code == 0

    # source wrote the integers into its own slot
    assert store.get_json("numbers", at=("src",)) == list(range(10))

    # detect produced 3-8 detections per integer, addressed via the output alias
    (scope, key), = runner.output_scopes().values()
    detections = store.get_json(key, at=scope)
    assert 30 <= len(detections) <= 80
    assert {d["number"] for d in detections} == set(range(10))
    assert all(0.0 <= d["score"] <= 1.0 for d in detections)


@pytest.mark.asyncio
async def test_args_reach_the_subprocess(tmp_path):
    """`args: { count: 10 }` on the dag node arrives as CASCADE_ARGS."""
    plan = _with_this_interpreter(_plan(FLAT))
    plan.node_graphs["main"].node("src").args = {"count": 4}
    _code, _runner, store = await _run(plan, tmp_path)
    assert store.get_json("numbers", at=("src",)) == [0, 1, 2, 3]


@pytest.mark.asyncio
async def test_the_store_layout_mirrors_the_dag(tmp_path):
    plan = _with_this_interpreter(_plan(FLAT))
    await _run(plan, tmp_path)
    written = sorted(
        str(p.relative_to(tmp_path)).replace("\\", "/")
        for p in tmp_path.rglob("*")
        if p.is_file()
    )
    assert written == ["mock/r1/main/det/detections", "mock/r1/main/src/numbers"]


@pytest.mark.asyncio
async def test_the_scattered_pipeline_runs(tmp_path):
    """M2. The target shape: detect fans over the integers, one subprocess per lane,
    and the fan closes at the node's own boundary."""
    plan = _with_this_interpreter(_plan(SCATTERED))
    code, runner, store = await _run(plan, tmp_path)
    assert code == 0

    (scope, key), = runner.output_scopes().values()
    # the fan wrote a descriptor; read resolves it, and a consumer cannot tell
    assert store.is_collection(key, at=scope)
    gathered = store.read_json(key, at=scope)

    # gather adds exactly one array level: 10 lanes, each a list of detections
    assert len(gathered) == 10
    assert all(isinstance(lane, list) for lane in gathered)
    assert all(3 <= len(lane) <= 8 for lane in gathered)

    # lane i handled element i -- ordering is positional, not completion order
    assert [lane[0]["number"] for lane in gathered] == list(range(10))


@pytest.mark.asyncio
async def test_each_lane_gets_its_own_slot_and_staged_element(tmp_path):
    """The one materialisation in the system: a scatter element does not exist as an
    artifact until the fan runner stages it."""
    plan = _with_this_interpreter(_plan(SCATTERED))
    await _run(plan, tmp_path)
    written = {
        str(p.relative_to(tmp_path)).replace("\\", "/")
        for p in tmp_path.rglob("*")
        if p.is_file()
    }
    assert "mock/r1/main/det/3/$in/number" in written   # staged element
    assert "mock/r1/main/det/3/detections" in written   # that lane's output
    assert "mock/r1/main/det/detections" in written     # the gathered collection


# --- cwd independence --------------------------------------------------------
# Two bugs made this worth pinning: a ref's relative command used to resolve against
# whatever directory the operator happened to be in, and once the child was given a cwd
# a *relative store root* meant different things to parent and child.

def _run_cli(cwd: Path, *argv: str) -> int:
    import os

    from cascade.cli.main import main

    previous = Path.cwd()
    try:
        os.chdir(cwd)
        return main(list(argv))
    finally:
        os.chdir(previous)


def test_runs_from_inside_the_project(tmp_path, capsys):
    """The route a real user takes."""
    assert _run_cli(EXAMPLES, "run", "pipeline_flat.yaml",
                    "--backend", "file", "--root", str(tmp_path)) == 0
    assert "complete" in capsys.readouterr().out


def test_runs_from_the_repo_root(tmp_path, capsys):
    """A ref's relative command resolves against the pipeline's directory, not the cwd."""
    assert _run_cli(ROOT, "run", "examples/mock/pipeline_flat.yaml",
                    "--backend", "file", "--root", str(tmp_path)) == 0
    assert "complete" in capsys.readouterr().out


def test_runs_from_an_unrelated_directory(tmp_path, capsys):
    unrelated = tmp_path / "elsewhere"
    unrelated.mkdir()
    assert _run_cli(unrelated, "run", str(EXAMPLES / "pipeline_flat.yaml"),
                    "--backend", "file", "--root", str(tmp_path / "store")) == 0
    assert "complete" in capsys.readouterr().out


def test_a_relative_store_root_resolves_against_the_operators_cwd(tmp_path, capsys):
    """The child runs elsewhere, so the root must be made absolute before it travels
    in CASCADE_STORE_OUT — otherwise parent and child write to different places."""
    work = tmp_path / "work"
    work.mkdir()
    assert _run_cli(work, "run", str(EXAMPLES / "pipeline_flat.yaml"),
                    "--backend", "file", "--root", "relstore") == 0
    assert (work / "relstore").is_dir()