"""``cascade.engine.executor`` — the shell around a run.

Covers what the executor owns that nothing else does: the store binding, the run id,
the ``$in`` convention, persisting the plan, and returning where outputs landed.
"""
import pytest
from yaml import safe_load

from cascade.deployment import Deployment
from cascade.model.pipeline import Pipeline
from cascade.plan.compile import compile_pipeline
from cascade.plan.plan import Plan
from cascade.engine.executor import (
    Executor,
    ExecutorError,
    RunResult,
    execute,
    new_run_id,
)
from cascade.store.file_store import FileConfig, FileStore


PIPELINE = """
entrypoint: main
input: [ { name: src, type: string } ]
types:
  structures:
    - name: Item
      fields: [ { name: k, type: string } ]
refs:
  - name: load
    runner: echo
    config: {}
    input:  [ { name: src,   type: string } ]
    output: [ { name: items, type: "Item[]" } ]
  - name: score
    runner: echo
    config: {}
    input:  [ { name: items,  type: "Item[]" } ]
    output: [ { name: scored, type: "Item[]" } ]
dags:
  - name: main
    input: [ { name: src, type: string } ]
    nodes:
      - name: load
        runs: load
        depends_on: [ { node: "$input", field: src, as: src } ]
      - name: score
        runs: score
        depends_on: [ { node: load, field: items, as: items } ]
    output: [ { node: score, field: scored, as: result } ]
"""


@pytest.fixture
def plan() -> Plan:
    return compile_pipeline(Pipeline.decode(safe_load(PIPELINE)))


@pytest.fixture
def store(tmp_path) -> FileStore:
    return FileStore(FileConfig(root=str(tmp_path), scope=("wilder",)))


# --- run ids -----------------------------------------------------------------

def test_run_ids_are_unique_within_a_second():
    """The suffix exists precisely so ids minted in the same second do not collide."""
    ids = [new_run_id() for _ in range(20)]
    assert len(set(ids)) == 20


def test_run_ids_sort_chronologically():
    """Lexicographic order == time order, so listing a bucket lists runs in order.
    Asserted on the timestamp prefix, since within one second the suffix is random."""
    import re

    rid = new_run_id()
    assert re.fullmatch(r"\d{8}T\d{6}-[0-9a-f]{6}", rid), rid
    earlier = "20250101T000000-aaaaaa"
    later = "20260101T000000-000000"
    assert sorted([later, earlier]) == [earlier, later]
    assert earlier < rid < "29990101T000000-000000"


# --- the run -----------------------------------------------------------------

@pytest.mark.asyncio
async def test_executes_a_pipeline_and_reports_its_outputs(plan, store, tmp_path):
    result = await Executor(plan, store=store).run(inputs={"src": "moths.wav"})

    assert isinstance(result, RunResult)
    assert result.entrypoint == "main"
    assert result.outputs == {"result": (("score",), "scored")}

    # fetch resolves the alias, so the caller never learns which node produced it
    assert isinstance(result.fetch("result"), list)


@pytest.mark.asyncio
async def test_a_supplied_run_id_is_used(plan, store, tmp_path):
    result = await Executor(plan, store=store).run(inputs={"src": "x"}, run_id="r1")
    assert result.run_id == "r1"
    assert (tmp_path / "wilder" / "r1" / "main" / "load" / "items").is_file()


@pytest.mark.asyncio
async def test_inputs_are_staged_under_the_in_convention(plan, store, tmp_path):
    await Executor(plan, store=store).run(inputs={"src": "moths.wav"}, run_id="r1")
    staged = tmp_path / "wilder" / "r1" / "main" / "$in" / "src"
    assert staged.is_file()
    assert "moths.wav" in staged.read_text()


@pytest.mark.asyncio
async def test_the_plan_is_written_into_the_run_scope(plan, store, tmp_path):
    """A completed run is self-describing: the graph sits beside the data."""
    await Executor(plan, store=store).run(inputs={"src": "x"}, run_id="r1")
    written = tmp_path / "wilder" / "r1" / "plan"
    assert written.is_file()

    import json
    assert Plan.decode(json.loads(written.read_text())) == plan


@pytest.mark.asyncio
async def test_two_runs_do_not_collide(plan, store, tmp_path):
    a = await Executor(plan, store=store).run(inputs={"src": "a"}, run_id="r1")
    b = await Executor(plan, store=store).run(inputs={"src": "b"}, run_id="r2")
    assert a.run_id != b.run_id
    assert (tmp_path / "wilder" / "r1" / "main" / "$in" / "src").read_text() != (
        tmp_path / "wilder" / "r2" / "main" / "$in" / "src"
    ).read_text()


# --- input validation --------------------------------------------------------

@pytest.mark.asyncio
async def test_missing_inputs_are_refused_before_anything_runs(plan, store, tmp_path):
    with pytest.raises(ExecutorError, match="missing input"):
        await Executor(plan, store=store).run(inputs={}, run_id="r1")
    # nothing was executed
    assert not (tmp_path / "wilder" / "r1" / "main" / "load").exists()


@pytest.mark.asyncio
async def test_unknown_inputs_are_refused(plan, store):
    with pytest.raises(ExecutorError, match="unknown input"):
        await Executor(plan, store=store).run(inputs={"src": "x", "nope": 1})


def test_an_executor_needs_a_store_or_a_deployment(plan):
    with pytest.raises(ExecutorError, match="deployment or a store"):
        Executor(plan)


# --- the deployment leg ------------------------------------------------------

@pytest.mark.asyncio
async def test_the_store_comes_from_the_deployment(plan, tmp_path):
    """The deployment is the only place the substrate is read."""
    deployment = Deployment(
        name="local",
        store=FileConfig(root=str(tmp_path), scope=("wilder", "moth")),
    )
    result = await Executor(plan, deployment=deployment).run(
        inputs={"src": "x"}, run_id="r1"
    )
    assert (tmp_path / "wilder" / "moth" / "r1" / "main" / "score" / "scored").is_file()
    assert result.fetch("result") is not None


@pytest.mark.asyncio
async def test_execute_is_a_one_shot_convenience(plan, store):
    result = await execute(plan, store=store, inputs={"src": "x"}, run_id="r1")
    assert result.run_id == "r1"
    assert "result" in result.outputs


@pytest.mark.asyncio
async def test_fetching_an_undeclared_port_is_an_error(plan, store):
    result = await Executor(plan, store=store).run(inputs={"src": "x"})
    with pytest.raises(ExecutorError, match="no output port"):
        result.fetch("nope")