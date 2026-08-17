"""Collections and the fan runner — items 1.5, 1.6, 2.4.

The reader is tolerant of both collection shapes, and the fan runner closes the fan
at the node's own boundary so nothing downstream knows a fan happened.
"""
import asyncio

import pytest

from cascade.engine.binding import InputBinding, InputBindings
from cascade.engine.collection import (
    CollectionDescriptor,
    CollectionError,
    collection_width,
    read_collection,
)
from cascade.engine.run_spec import RunSpec
from cascade.engine.runner.runner_coro import RunnerCoro
from cascade.engine.runner.runner_fan import FanError, FanRunner
from cascade.store.file_store import FileConfig, FileStore


# --- 1.5 / 1.6: the two shapes ----------------------------------------------

def test_reads_a_monolithic_collection(tmp_path):
    """For JSON a monolithic collection is already a list once parsed, which is why
    the codec (1.7) is not on the critical path."""
    store = FileStore(FileConfig(root=str(tmp_path)))
    store.put_json("numbers", [0, 1, 2])
    assert read_collection(store, "numbers") == [0, 1, 2]
    assert collection_width(store, "numbers") == 3


def test_reads_a_distributed_collection(tmp_path):
    store = FileStore(FileConfig(root=str(tmp_path)))
    for i, value in enumerate(["a", "b"]):
        store.put_json("item", value, at=("part", str(i)))
    descriptor = CollectionDescriptor(
        count=2, elements=(("part", "0"), ("part", "1")), key="item"
    )
    store.put_json("things", descriptor.encode())

    assert read_collection(store, "things") == ["a", "b"]
    assert collection_width(store, "things") == 2


def test_width_of_a_descriptor_does_not_read_the_elements(tmp_path):
    """The count is in the descriptor, so asking how wide a fan is stays cheap."""
    store = FileStore(FileConfig(root=str(tmp_path)))
    store.put_json(
        "things",
        CollectionDescriptor(count=500, elements=(("nowhere",),) * 500, key="x").encode(),
    )
    assert collection_width(store, "things") == 500  # would raise if it read them


def test_descriptor_round_trips():
    d = CollectionDescriptor(count=2, elements=(("a",), ("b",)), key="k", element_type="Det")
    assert CollectionDescriptor.decode(d.encode()) == d
    assert CollectionDescriptor.looks_like(d.encode())
    assert not CollectionDescriptor.looks_like([1, 2])


def test_a_non_collection_is_an_error(tmp_path):
    store = FileStore(FileConfig(root=str(tmp_path)))
    store.put_json("scalar", 7)
    with pytest.raises(CollectionError):
        read_collection(store, "scalar")


# --- 2.4: the fan -----------------------------------------------------------

def _fan_setup(tmp_path, elements, outputs={"out": ((), "out")}):
    """A dag-scoped reader with an upstream collection, and a node-scoped writer."""
    base = FileConfig(root=str(tmp_path), scope=("r1", "main"))
    reader = FileStore(base)
    writer = FileStore(base.subscope(("det",)))
    reader.put_json("numbers", elements, at=("src",))
    spec = RunSpec(
        name="detect", run_id="r1", node_id="det", instance_id="r1/main/det",
        store_in=reader, store_out=writer,
        inputs=InputBindings(
            inputs=(InputBinding(port="number", scope=("src",), key="numbers"),)
        ),
    )
    return spec, reader, writer


async def _double(spec: RunSpec) -> int:
    """A stand-in ref: reads its staged element, writes twice it."""
    binding = spec.inputs.input_for("number")
    value = spec.store_in.get_json(binding.key, at=binding.scope)
    spec.store_out.put_json("out", value * 2)
    return 0


@pytest.mark.asyncio
async def test_fan_runs_one_lane_per_element_and_gathers(tmp_path):
    spec, _reader, writer = _fan_setup(tmp_path, [1, 2, 3])
    runner = FanRunner(child=RunnerCoro(coro=_double), scatter_port="number", outputs={"out": ((), "out")})
    assert await runner.run(spec) == 0

    # the fan closed: one artifact at the node's own scope, one array level deeper
    assert writer.get_json("out") == [2, 4, 6]
    # and each lane kept its own slot
    assert [writer.get_json("out", at=(str(i),)) for i in range(3)] == [2, 4, 6]


@pytest.mark.asyncio
async def test_lane_order_is_positional_not_completion_order(tmp_path):
    """Lane i must stay element i however the lanes interleave — as_completed would
    silently destroy the correspondence the gathered collection depends on."""
    async def slow_for_early_elements(spec: RunSpec) -> int:
        binding = spec.inputs.input_for("number")
        value = spec.store_in.get_json(binding.key, at=binding.scope)
        await asyncio.sleep((10 - value) / 100)  # earlier elements finish last
        spec.store_out.put_json("out", value)
        return 0

    spec, _reader, writer = _fan_setup(tmp_path, [0, 1, 2, 3, 4])
    runner = FanRunner(
        child=RunnerCoro(coro=slow_for_early_elements), scatter_port="number", outputs={"out": ((), "out")}
    )
    await runner.run(spec)
    assert writer.get_json("out") == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_the_element_is_staged_for_each_lane(tmp_path):
    spec, _reader, writer = _fan_setup(tmp_path, [7, 8])
    await FanRunner(
        child=RunnerCoro(coro=_double), scatter_port="number", outputs={"out": ((), "out")}
    ).run(spec)
    assert writer.get_json("number", at=("0", "$in")) == 7
    assert writer.get_json("number", at=("1", "$in")) == 8


@pytest.mark.asyncio
async def test_an_empty_collection_fans_zero_lanes(tmp_path):
    """N=0 must produce an empty collection, not an error — a classic silent
    wrong-answer case."""
    spec, _reader, writer = _fan_setup(tmp_path, [])
    assert await FanRunner(
        child=RunnerCoro(coro=_double), scatter_port="number", outputs={"out": ((), "out")}
    ).run(spec) == 0
    assert writer.get_json("out") == []


@pytest.mark.asyncio
async def test_a_single_element_still_yields_a_collection(tmp_path):
    """Width 1 is a one-element array, never a bare value."""
    spec, _reader, writer = _fan_setup(tmp_path, [5])
    await FanRunner(
        child=RunnerCoro(coro=_double), scatter_port="number", outputs={"out": ((), "out")}
    ).run(spec)
    assert writer.get_json("out") == [10]


@pytest.mark.asyncio
async def test_a_failing_lane_fails_the_fan(tmp_path):
    async def fails_on_two(spec: RunSpec) -> int:
        binding = spec.inputs.input_for("number")
        value = spec.store_in.get_json(binding.key, at=binding.scope)
        return 1 if value == 2 else 0

    spec, _reader, _writer = _fan_setup(tmp_path, [1, 2, 3])
    with pytest.raises(FanError, match="exited 1"):
        await FanRunner(
            child=RunnerCoro(coro=fails_on_two), scatter_port="number", outputs={"out": ((), "out")}
        ).run(spec)


@pytest.mark.asyncio
async def test_max_concurrent_bounds_the_lanes(tmp_path):
    live = 0
    peak = 0

    async def watched(spec: RunSpec) -> int:
        nonlocal live, peak
        live += 1
        peak = max(peak, live)
        await asyncio.sleep(0.01)
        live -= 1
        spec.store_out.put_json("out", 1)
        return 0

    spec, _reader, _writer = _fan_setup(tmp_path, list(range(8)))
    await FanRunner(
        child=RunnerCoro(coro=watched), scatter_port="number",
        outputs={"out": ((), "out")}, max_concurrent=2,
    ).run(spec)
    assert peak <= 2


@pytest.mark.asyncio
async def test_unset_max_concurrent_is_unbounded_by_design(tmp_path):
    """Under a broker the scheduler owns the budget; a local cap would throttle work
    it cannot see."""
    live = 0
    peak = 0

    async def watched(spec: RunSpec) -> int:
        nonlocal live, peak
        live += 1
        peak = max(peak, live)
        await asyncio.sleep(0.01)
        live -= 1
        spec.store_out.put_json("out", 1)
        return 0

    spec, _reader, _writer = _fan_setup(tmp_path, list(range(8)))
    await FanRunner(
        child=RunnerCoro(coro=watched), scatter_port="number", outputs={"out": ((), "out")}
    ).run(spec)
    assert peak == 8


@pytest.mark.asyncio
async def test_an_unbound_scatter_port_is_an_error(tmp_path):
    spec, _reader, _writer = _fan_setup(tmp_path, [1])
    spec.inputs = InputBindings()
    with pytest.raises(FanError, match="no input is bound"):
        await FanRunner(
            child=RunnerCoro(coro=_double), scatter_port="number", outputs={"out": ((), "out")}
        ).run(spec)


@pytest.mark.asyncio
async def test_unimplemented_merge_is_refused_loudly(tmp_path):
    spec, _reader, _writer = _fan_setup(tmp_path, [1])
    with pytest.raises(FanError, match="not implemented"):
        await FanRunner(
            child=RunnerCoro(coro=_double), scatter_port="number",
            outputs={"out": ((), "out")}, merge="dict",
        ).run(spec)


# --- a fanned subdag: multi-stage per-element work ---------------------------

CASE_B = """
entrypoint: main
input: []
types:
  structures:
    - name: Detection
      fields: [ { name: n, type: int } ]
refs:
  - { name: lister, runner: echo, config: {}, input: [], output: [ { name: images, type: "string[]" } ] }
  - { name: detect, runner: echo, config: {}, input: [ { name: image, type: string } ], output: [ { name: dets, type: "Detection[]" } ] }
  - { name: refine, runner: echo, config: {}, input: [ { name: dets, type: "Detection[]" } ], output: [ { name: clean, type: "Detection[]" } ] }
dags:
  - name: perimage
    input: [ { name: image, type: string } ]
    nodes:
      - { name: d, runs: detect, depends_on: [ { node: "$input", field: image, as: image } ] }
      - { name: r, runs: refine, depends_on: [ { node: d, field: dets, as: dets } ] }
    output: [ { node: r, field: clean, as: clean } ]
  - name: main
    input: []
    nodes:
      - { name: l, runs: lister }
      - { name: p, runs: perimage, scatter: image, depends_on: [ { node: l, field: images, as: image } ] }
    output: [ { node: p, field: clean, as: results } ]
"""


@pytest.mark.asyncio
async def test_a_fanned_subdag_runs_every_stage_per_element(tmp_path):
    """Multi-stage per-element work needs no new machinery: wrap the stages in a
    subdag and scatter that. FanRunner(DagRunner(...)) fans it opaquely."""
    from yaml import safe_load
    from cascade.model.pipeline import Pipeline
    from cascade.plan.compile import compile_pipeline
    from cascade.engine.runner.runner_dag import DagRunner

    plan = compile_pipeline(Pipeline.decode(safe_load(CASE_B)))
    store = FileStore(FileConfig(root=str(tmp_path), scope=("r1", "main")))
    runner = DagRunner("main", plan)
    assert await runner.run(
        RunSpec(name="main", run_id="r1", instance_id="r1/main",
                store_out=store, inputs=InputBindings())
    ) == 0

    # both stages ran in every lane, each in its own slot
    written = {
        str(p.relative_to(tmp_path)).replace("\\", "/") for p in tmp_path.rglob("*") if p.is_file()
    }
    for lane in range(3):
        assert f"r1/main/p/{lane}/$in/image" in written   # staged element
        assert f"r1/main/p/{lane}/d/dets" in written      # stage 1
        assert f"r1/main/p/{lane}/r/clean" in written     # stage 2

    # the fan closed at the node, following the subdag's own output alias
    assert "r1/main/p/clean" in written
    gathered = store.get_json("clean", at=("p",))
    assert len(gathered) == 3
    assert runner.output_scopes() == {"results": (("p",), "clean")}