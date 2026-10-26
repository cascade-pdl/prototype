"""``Runner.leaves`` — the launches a run would make, listed without running it.

What matters is that the listing cannot drift from the run: each leaf's spec must be
exactly a spec the run hands that ref, through nested dags, staged dag inputs and fans.
So the run is recorded and compared, not re-derived by hand here.
"""
import pytest
from yaml import safe_load

import cascade.engine.fan as fan
from cascade.engine.dag import DagRunner
from cascade.model.pipeline import Pipeline
from cascade.plan.compile import compile_pipeline
from cascade.protocol.binding import InputBinding, InputBindings
from cascade.protocol.coro import RunnerCoroBase
from cascade.protocol.run_spec import RunSpec, to_env
from cascade.runners.build import configure_ref_builder
from cascade.store.file_store import FileConfig, FileStore
from cascade.types import TypeExpr


# gen -> per (a fanned subdag: stage1 -> stage2) and gen -> each (a fanned ref), with a
# dag input passed down into the subdag
NESTED = """
entrypoint: main
input: [ { name: src, type: string } ]
refs:
  - name: gen
    runner: echo
    config: {}
    input:  [ { name: src, type: string } ]
    output: [ { name: items, type: "string[]" } ]
  - name: step
    runner: echo
    config: {}
    input:  [ { name: item, type: string }, { name: src, type: string } ]
    output: [ { name: out, type: string } ]
  - name: tail
    runner: echo
    config: {}
    input:  [ { name: item, type: string } ]
    output: [ { name: out, type: string } ]
dags:
  - name: inner
    input: [ { name: item, type: string }, { name: src, type: string } ]
    nodes:
      - name: stage1
        runs: step
        args: { level: 1 }
        depends_on:
          - { node: "$input", field: item, as: item }
          - { node: "$input", field: src, as: src }
      - name: stage2
        runs: tail
        depends_on: [ { node: stage1, field: out, as: item } ]
    output: [ { node: stage2, field: out, as: out } ]
  - name: main
    input: [ { name: src, type: string } ]
    nodes:
      - name: gen
        runs: gen
        depends_on: [ { node: "$input", field: src, as: src } ]
      - name: per
        runs: inner
        scatter: item
        depends_on:
          - { node: gen, field: items, as: item }
          - { node: "$input", field: src, as: src }
      - name: each
        runs: tail
        scatter: item
        depends_on: [ { node: gen, field: items, as: item } ]
    output: [ { node: per, field: out, as: out } ]
"""


class Recorder(RunnerCoroBase):
    """Records the spec of every launch, then runs the real ref."""

    def __init__(self, inner, seen: list[RunSpec]):
        self.inner, self.seen = inner, seen

    def _invoke(self, spec: RunSpec):
        self.seen.append(spec)
        return self.inner.run(spec)


def _root(tmp_path) -> RunSpec:
    store = FileStore(FileConfig(root=str(tmp_path), scope=("r1", "main")))
    store.put_json("src", "moths.wav", at=("$in",))
    return RunSpec(
        name="main", run_id="r1", instance_id="r1/main", store_out=store,
        inputs=InputBindings(inputs=(
            InputBinding(port="src", scope=("$in",), key="src", type=TypeExpr.parse("string")),
        )),
    )


@pytest.mark.asyncio
async def test_each_leaf_is_a_launch_the_run_makes(tmp_path, monkeypatch):
    # lane 0 stands for every lane; the run's lane 0 must then match it exactly
    monkeypatch.setattr(fan, "WIDEST_LANE", 0)
    plan = compile_pipeline(Pipeline.decode(safe_load(NESTED)))
    seen: list[RunSpec] = []
    build = configure_ref_builder()

    def recording(name, config, signature=None):
        return Recorder(build(name, config, signature), seen)

    runner = DagRunner("main", plan, recording)
    root = _root(tmp_path)
    leaves = [spec for _runner, spec in runner.leaves(root)]
    assert await runner.run(root) == 0

    assert sorted(s.instance_id for s in leaves) == [
        "r1/main/each/0", "r1/main/gen", "r1/main/per/0/stage1", "r1/main/per/0/stage2",
    ]
    launched = [to_env(s) for s in seen]
    for leaf in leaves:
        assert to_env(leaf) in launched, leaf.instance_id


def test_listing_leaves_reads_and_writes_nothing(tmp_path):
    plan = compile_pipeline(Pipeline.decode(safe_load(NESTED)))
    root = _root(tmp_path)
    before = sorted(p for p in tmp_path.rglob("*"))
    leaves = list(DagRunner("main", plan).leaves(root))
    assert len(leaves) == 4
    assert sorted(p for p in tmp_path.rglob("*")) == before


def test_a_fan_leaf_assumes_the_widest_lane(tmp_path):
    plan = compile_pipeline(Pipeline.decode(safe_load(NESTED)))
    leaves = {s.node_id: s for _r, s in DagRunner("main", plan).leaves(_root(tmp_path))}
    assert leaves["each"].instance_id == f"r1/main/each/{fan.WIDEST_LANE}"
