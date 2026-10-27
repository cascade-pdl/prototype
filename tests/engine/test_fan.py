"""``FanRunner`` — one node, once per element, and the fan closed at its boundary.

The gathered result is a collection *descriptor*: nothing is copied, and a consumer
calling ``store.read`` cannot tell that from a materialised list. Collection mechanics
themselves are tested in ``tests/store/test_collection.py``, where they now live.
"""
import asyncio

import pytest

from cascade.protocol.binding import InputBinding, InputBindings
from cascade.types import TypeExpr
from cascade.protocol.run_spec import RunSpec
from cascade.runners.coro import RunnerCoro
from cascade.engine.fan import FanError, FanRunner
from cascade.store.file_store import FileConfig, FileStore


# --- 2.4: the fan -----------------------------------------------------------

async def _fan_setup(tmp_path, elements, outputs={"out": ((), "out")}):
    """A dag-scoped reader with an upstream collection, and a node-scoped writer."""
    base = FileConfig(root=str(tmp_path), scope=("r1", "main"))
    reader = FileStore(base)
    writer = FileStore(base.subscope(("det",)))
    await reader.put_json("numbers", elements, at=("src",))
    spec = RunSpec(
        name="detect", run_id="r1", node_id="det", instance_id="r1/main/det",
        store_in=reader, store_out=writer,
        inputs=InputBindings(
            inputs=(InputBinding(port="number", scope=("src",), key="numbers",
                                 type=TypeExpr.parse("int")),)
        ),
    )
    return spec, reader, writer


async def _double(spec: RunSpec) -> int:
    """A stand-in ref: reads its staged element, writes twice it."""
    binding = spec.inputs.input_for("number")
    value = await spec.store_in.get_json(binding.key, at=binding.scope)
    await spec.store_out.put_json("out", value * 2)
    return 0


@pytest.mark.asyncio
async def test_fan_runs_one_lane_per_element_and_gathers(tmp_path):
    spec, _reader, writer = await _fan_setup(tmp_path, [1, 2, 3])
    runner = FanRunner(child=RunnerCoro(coro=_double), scatter_port="number", outputs={"out": ((), "out")})
    assert await runner.run(spec) == 0

    # the fan closed: one artifact at the node's own scope, one array level deeper
    assert await writer.read_json("out") == [2, 4, 6]           # resolves the descriptor
    assert await writer.is_collection("out")                # ...which is what was written
    # and each lane kept its own slot
    assert [await writer.get_json("out", at=(str(i),)) for i in range(3)] == [2, 4, 6]


@pytest.mark.asyncio
async def test_lane_order_is_positional_not_completion_order(tmp_path):
    """Lane i must stay element i however the lanes interleave — as_completed would
    silently destroy the correspondence the gathered collection depends on."""
    async def slow_for_early_elements(spec: RunSpec) -> int:
        binding = spec.inputs.input_for("number")
        value = await spec.store_in.get_json(binding.key, at=binding.scope)
        await asyncio.sleep((10 - value) / 100)  # earlier elements finish last
        await spec.store_out.put_json("out", value)
        return 0

    spec, _reader, writer = await _fan_setup(tmp_path, [0, 1, 2, 3, 4])
    runner = FanRunner(
        child=RunnerCoro(coro=slow_for_early_elements), scatter_port="number", outputs={"out": ((), "out")}
    )
    await runner.run(spec)
    assert await writer.read_json("out") == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_the_element_is_staged_for_each_lane(tmp_path):
    spec, _reader, writer = await _fan_setup(tmp_path, [7, 8])
    await FanRunner(
        child=RunnerCoro(coro=_double), scatter_port="number", outputs={"out": ((), "out")}
    ).run(spec)
    assert await writer.get_json("number", at=("0", "$in")) == 7
    assert await writer.get_json("number", at=("1", "$in")) == 8


async def _gathered_setup(tmp_path, values):
    """Like ``_fan_setup``, but the upstream is a *distributed* collection, as a fan
    gathers it: one object per element under ``src/<i>/v`` and a descriptor at
    ``src/numbers``. Store calls on the reader and the fan's own writer are recorded."""
    spec, reader, writer = await _fan_setup(tmp_path, [])
    for i, value in enumerate(values):
        await reader.put_json("v", value, at=("src", str(i)))
    await reader.write_collection("numbers", [((str(i),), "v") for i in range(len(values))], at=("src",))
    calls = []
    for name, store in (("reader", reader), ("writer", writer)):
        for method in ("get", "put"):
            original = getattr(store, method)

            def recorded(*args, _original=original, _call=(name, method), **kwargs):
                calls.append(_call)
                return _original(*args, **kwargs)

            setattr(store, method, recorded)
    return spec, reader, writer, calls


@pytest.mark.asyncio
async def test_a_lane_is_bound_to_a_gathered_element_where_it_is(tmp_path):
    """Over a distributed collection the fan reads the descriptor and nothing else:
    lane i reads element i in place, and no element is copied."""
    bound = {}

    async def records_its_binding(spec: RunSpec) -> int:
        binding = spec.inputs.input_for("number")
        value = await spec.store_in.get_json(binding.key, at=binding.scope)
        bound[value] = (binding.scope, binding.key)
        await spec.store_out.put_json("out", value * 2)
        return 0

    spec, _reader, writer, calls = await _gathered_setup(tmp_path, [7, 8, 9])
    await FanRunner(
        child=RunnerCoro(coro=records_its_binding), scatter_port="number",
        outputs={"out": ((), "out")},
    ).run(spec)

    assert bound == {7: (("src", "0"), "v"), 8: (("src", "1"), "v"), 9: (("src", "2"), "v")}
    assert await writer.read_json("out") == [14, 16, 18]
    # the fan: one read (the descriptor) and one write (the gather); each lane its read
    assert calls.count(("reader", "get")) == 1 + 3
    assert calls.count(("writer", "put")) == 1
    assert not any("$in" in str(p) for p in tmp_path.rglob("*"))


@pytest.mark.asyncio
async def test_a_gathered_element_that_is_a_collection_reads_back_as_one(tmp_path):
    """An element may itself be a descriptor (a fan of fans): bound in place, the lane
    still reads a list."""
    spec, reader, _writer, _calls = await _gathered_setup(tmp_path, [])
    for lane in range(2):
        for j in range(2):
            await reader.put_json("v", lane * 10 + j, at=("src", str(lane), "parts", str(j)))
        await reader.write_collection("v", [(("parts", "0"), "v"), (("parts", "1"), "v")], at=("src", str(lane)))
    await reader.write_collection("numbers", [(("0",), "v"), (("1",), "v")], at=("src",))

    seen = []

    async def reads(spec: RunSpec) -> int:
        binding = spec.inputs.input_for("number")
        seen.append(await spec.store_in.read_json(binding.key, at=binding.scope))
        await spec.store_out.put_json("out", 0)
        return 0

    await FanRunner(child=RunnerCoro(coro=reads), scatter_port="number", outputs={"out": ((), "out")}).run(spec)
    assert sorted(seen) == [[0, 1], [10, 11]]


@pytest.mark.asyncio
async def test_scattering_over_a_single_value_is_an_error(tmp_path):
    spec, _reader, _writer = await _fan_setup(tmp_path, 5)
    with pytest.raises(FanError, match="src/numbers is not a collection"):
        await FanRunner(
            child=RunnerCoro(coro=_double), scatter_port="number", outputs={"out": ((), "out")}
        ).run(spec)


@pytest.mark.asyncio
async def test_an_empty_collection_fans_zero_lanes(tmp_path):
    """N=0 must produce an empty collection, not an error — a classic silent
    wrong-answer case."""
    spec, _reader, writer = await _fan_setup(tmp_path, [])
    assert await FanRunner(
        child=RunnerCoro(coro=_double), scatter_port="number", outputs={"out": ((), "out")}
    ).run(spec) == 0
    assert await writer.read_json("out") == []


@pytest.mark.asyncio
async def test_a_single_element_still_yields_a_collection(tmp_path):
    """Width 1 is a one-element array, never a bare value."""
    spec, _reader, writer = await _fan_setup(tmp_path, [5])
    await FanRunner(
        child=RunnerCoro(coro=_double), scatter_port="number", outputs={"out": ((), "out")}
    ).run(spec)
    assert await writer.read_json("out") == [10]


@pytest.mark.asyncio
async def test_a_failing_lane_fails_the_fan(tmp_path):
    async def fails_on_two(spec: RunSpec) -> int:
        binding = spec.inputs.input_for("number")
        value = await spec.store_in.get_json(binding.key, at=binding.scope)
        return 1 if value == 2 else 0

    spec, _reader, _writer = await _fan_setup(tmp_path, [1, 2, 3])
    with pytest.raises(FanError, match="exited 1"):
        await FanRunner(
            child=RunnerCoro(coro=fails_on_two), scatter_port="number", outputs={"out": ((), "out")}
        ).run(spec)


@pytest.mark.asyncio
async def test_a_fan_starts_every_lane_at_once(tmp_path):
    """Coordination limits nothing: how many run at once is the leaves' slots."""
    live = 0
    peak = 0

    async def watched(spec: RunSpec) -> int:
        nonlocal live, peak
        live += 1
        peak = max(peak, live)
        await asyncio.sleep(0.01)
        live -= 1
        await spec.store_out.put_json("out", 1)
        return 0

    spec, _reader, _writer = await _fan_setup(tmp_path, list(range(8)))
    await FanRunner(
        child=RunnerCoro(coro=watched), scatter_port="number", outputs={"out": ((), "out")}
    ).run(spec)
    assert peak == 8


@pytest.mark.asyncio
async def test_an_unbound_scatter_port_is_an_error(tmp_path):
    spec, _reader, _writer = await _fan_setup(tmp_path, [1])
    spec.inputs = InputBindings()
    with pytest.raises(FanError, match="no input is bound"):
        await FanRunner(
            child=RunnerCoro(coro=_double), scatter_port="number", outputs={"out": ((), "out")}
        ).run(spec)


# --- merge: nest vs flatten --------------------------------------------------

async def _listify(spec: RunSpec) -> int:
    """A stand-in ref whose output is itself a collection, so it can be flattened."""
    binding = spec.inputs.input_for("number")
    value = await spec.store_in.read_json(binding.key, at=binding.scope)
    await spec.store_out.put_json("out", [value, value * 10])
    return 0


@pytest.mark.asyncio
async def test_nest_adds_one_array_level(tmp_path):
    spec, _reader, writer = await _fan_setup(tmp_path, [1, 2])
    await FanRunner(
        child=RunnerCoro(coro=_listify), scatter_port="number",
        outputs={"out": ((), "out")}, merge="nest",
    ).run(spec)
    assert await writer.read_json("out") == [[1, 10], [2, 20]]


@pytest.mark.asyncio
async def test_flatten_concatenates_lane_collections(tmp_path):
    """Depth unchanged rather than +1 — and when every lane wrote a descriptor this is
    metadata only, so no payload moves."""
    spec, _reader, writer = await _fan_setup(tmp_path, [1, 2])
    await FanRunner(
        child=RunnerCoro(coro=_listify), scatter_port="number",
        outputs={"out": ((), "out")}, merge="flatten",
    ).run(spec)
    assert await writer.read_json("out") == [1, 10, 2, 20]


@pytest.mark.asyncio
async def test_flatten_of_distributed_lanes_moves_no_payload(tmp_path):
    """Each lane writes a descriptor; the merged result references the same elements."""
    async def writes_a_collection(spec: RunSpec) -> int:
        binding = spec.inputs.input_for("number")
        value = await spec.store_in.read_json(binding.key, at=binding.scope)
        await spec.store_out.put_json("e", value, at=("parts", "0"))
        await spec.store_out.put_json("e", value * 10, at=("parts", "1"))
        await spec.store_out.write_collection("out", [(("parts", "0"), "e"), (("parts", "1"), "e")])
        return 0

    spec, _reader, writer = await _fan_setup(tmp_path, [1, 2])
    await FanRunner(
        child=RunnerCoro(coro=writes_a_collection), scatter_port="number",
        outputs={"out": ((), "out")}, merge="flatten",
    ).run(spec)
    assert await writer.is_collection("out")           # still a descriptor: nothing copied
    assert await writer.read_json("out") == [1, 10, 2, 20]


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
    from cascade.engine.dag import DagRunner

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
    gathered = await store.read_json("clean", at=("p",))   # descriptor -> the lanes' outputs
    assert len(gathered) == 3
    assert runner.output_scopes() == {"results": (("p",), "clean")}

CASE_C = """
entrypoint: main
input: []
refs:
  - { name: lister, runner: echo, config: {}, input: [], output: [ { name: images, type: "string[]" } ] }
  - { name: weights, runner: echo, config: {}, input: [], output: [ { name: model, type: string } ] }
  - { name: detect, runner: echo, config: {}, input: [ { name: image, type: string }, { name: model, type: string } ], output: [ { name: dets, type: string } ] }
  - { name: count, runner: echo, config: {}, input: [ { name: all, type: "string[]" } ], output: [ { name: n, type: string } ] }
dags:
  - name: perimage
    input: [ { name: image, type: string }, { name: model, type: string } ]
    nodes:
      - { name: d, runs: detect, depends_on: [ { node: "$input", field: image, as: image }, { node: "$input", field: model, as: model } ] }
    output: [ { node: d, field: dets, as: dets } ]
  - name: summary
    input: [ { name: all, type: "string[]" } ]
    nodes:
      - { name: c, runs: count, depends_on: [ { node: "$input", field: all, as: all } ] }
    output: [ { node: c, field: n, as: n } ]
  - name: main
    input: []
    nodes:
      - { name: l, runs: lister }
      - { name: w, runs: weights }
      - { name: p, runs: perimage, scatter: image, depends_on: [ { node: l, field: images, as: image }, { node: w, field: model, as: model } ] }
      - { name: s, runs: summary, depends_on: [ { node: p, field: dets, as: all } ] }
    output: [ { node: s, field: n, as: n } ]
"""


@pytest.mark.asyncio
async def test_a_fanned_subdag_stages_broadcast_inputs_per_lane_and_a_gathered_collection_survives_staging(tmp_path):
    """The non-scattered input is copied into every lane's $in (the known cost of
    staging); the scattered element is already there and is not copied again. The
    fan's gathered descriptor, fed into another subdag, is copied with its elements,
    so it reads back the same from inside."""
    from yaml import safe_load
    from cascade.model.pipeline import Pipeline
    from cascade.plan.compile import compile_pipeline
    from cascade.engine.dag import DagRunner

    plan = compile_pipeline(Pipeline.decode(safe_load(CASE_C)))
    store = FileStore(FileConfig(root=str(tmp_path), scope=("r1", "main")))
    assert await DagRunner("main", plan).run(
        RunSpec(name="main", run_id="r1", instance_id="r1/main",
                store_out=store, inputs=InputBindings())
    ) == 0

    for lane in range(3):
        assert await store.get_json("model", at=("p", str(lane), "$in")) == await store.get_json("model", at=("w",))
        assert await store.has("dets", at=("p", str(lane), "d"))

    assert await store.is_collection("dets", at=("p",))
    assert await store.is_collection("all", at=("s", "$in"))
    assert await store.read_json("all", at=("s", "$in")) == await store.read_json("dets", at=("p",))
    assert await store.has("n", at=("s", "c"))


@pytest.mark.asyncio
async def test_a_failing_lane_stops_the_other_lanes_at_once(tmp_path):
    """Fail fast across a fan: lane 1 fails while the others are still running; they
    are cancelled (on ECS: stopped) instead of being waited for."""
    stopped = []

    async def lane(spec: RunSpec) -> int:
        binding = spec.inputs.input_for("number")
        value = await spec.store_in.get_json(binding.key, at=binding.scope)
        if value == 2:
            await asyncio.sleep(0.02)
            return 1
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            stopped.append(value)
            raise
        return 0

    spec, _reader, writer = await _fan_setup(tmp_path, [1, 2, 3])
    with pytest.raises(FanError, match=r"r1/main/det/1: exited 1"):
        await asyncio.wait_for(
            FanRunner(child=RunnerCoro(coro=lane), scatter_port="number", outputs={"out": ((), "out")}).run(spec),
            timeout=5,
        )
    assert sorted(stopped) == [1, 3]
    assert not await writer.has("out")  # a failed fan gathers nothing


CASE_D = """
entrypoint: main
input: []
refs:
  - { name: lister, runner: echo, config: {}, input: [], output: [ { name: images, type: "string[]" } ] }
  - { name: detect, runner: echo, config: {}, input: [ { name: image, type: string } ], output: [ { name: dets, type: string } ] }
  - { name: refine, runner: echo, config: {}, input: [ { name: dets, type: string } ], output: [ { name: clean, type: string } ] }
dags:
  - name: perdet
    input: [ { name: dets, type: string } ]
    nodes:
      - { name: r, runs: refine, depends_on: [ { node: "$input", field: dets, as: dets } ] }
    output: [ { node: r, field: clean, as: clean } ]
  - name: main
    input: []
    nodes:
      - { name: l, runs: lister }
      - { name: d, runs: detect, scatter: image, depends_on: [ { node: l, field: images, as: image } ] }
      - { name: p, runs: perdet, scatter: dets, depends_on: [ { node: d, field: dets, as: dets } ] }
    output: [ { node: p, field: clean, as: clean } ]
"""


@pytest.mark.asyncio
async def test_a_fan_over_a_gathered_collection_copies_only_into_subdag_lanes(tmp_path):
    """``d`` fans a ref over ``l``'s single list: its elements are staged. ``p`` fans a
    subdag over ``d``'s gathered output: the fan copies nothing, but each lane, being a
    dag, copies its element into its own ``$in``, where its inner node reads it."""
    from yaml import safe_load
    from cascade.model.pipeline import Pipeline
    from cascade.plan.compile import compile_pipeline
    from cascade.engine.dag import DagRunner

    plan = compile_pipeline(Pipeline.decode(safe_load(CASE_D)))
    store = FileStore(FileConfig(root=str(tmp_path), scope=("r1", "main")))
    assert await DagRunner("main", plan).run(
        RunSpec(name="main", run_id="r1", instance_id="r1/main",
                store_out=store, inputs=InputBindings())
    ) == 0

    for lane in range(3):
        assert await store.has("image", at=("d", str(lane), "$in"))     # split from a list
        assert await store.get_json("dets", at=("p", str(lane), "$in")) == await store.get_json(
            "dets", at=("d", str(lane))
        )
        assert await store.has("clean", at=("p", str(lane), "r"))


@pytest.mark.asyncio
async def test_the_lanes_of_a_fanned_subdag_copy_their_elements_at_once(tmp_path, monkeypatch):
    """Each lane's boundary copy is a store call the loop does not wait on, so the
    lanes' copies into ``$in`` overlap instead of queueing behind one another."""
    import threading
    import time
    from yaml import safe_load
    from cascade.model.pipeline import Pipeline
    from cascade.plan.compile import compile_pipeline
    from cascade.engine.dag import DagRunner

    live = peak = 0
    lock = threading.Lock()
    original = FileStore._put

    def slow_staging(self, key, data, at=()):
        nonlocal live, peak
        if "$in" not in (*self.scope, *at) or "p" not in self.scope:
            return original(self, key, data, at)
        with lock:
            live += 1
            peak = max(peak, live)
        try:
            time.sleep(0.1)
            return original(self, key, data, at)
        finally:
            with lock:
                live -= 1

    monkeypatch.setattr(FileStore, "_put", slow_staging)
    plan = compile_pipeline(Pipeline.decode(safe_load(CASE_D)))
    store = FileStore(FileConfig(root=str(tmp_path), scope=("r1", "main")))
    assert await DagRunner("main", plan).run(
        RunSpec(name="main", run_id="r1", instance_id="r1/main",
                store_out=store, inputs=InputBindings())
    ) == 0
    assert peak == 3


@pytest.mark.asyncio
async def test_a_lane_is_told_the_ports_declared_type_not_one_level_less(tmp_path):
    """A scattered port declares what one lane receives. Over ``string[][]`` a port
    declared ``string[]`` gets a list per lane, and its binding must say depth 1: the
    node SDK checks the value against it (a scalar element hid this)."""
    from yaml import safe_load
    from cascade.model.pipeline import Pipeline
    from cascade.plan.compile import compile_pipeline
    from cascade.engine.dag import DagRunner

    pipeline = """
entrypoint: main
input: []
refs:
  - { name: make, runner: echo, config: {}, input: [], output: [ { name: groups, type: "string[][]" } ] }
  - { name: take, runner: echo, config: {}, input: [ { name: group, type: "string[]" } ],
      output: [ { name: n, type: string } ] }
dags:
  - name: main
    input: []
    nodes:
      - { name: m, runs: make }
      - { name: t, runs: take, scatter: group, depends_on: [ { node: m, field: groups, as: group } ] }
    output: [ { node: t, field: n, as: n } ]
"""
    plan = compile_pipeline(Pipeline.decode(safe_load(pipeline)))
    seen = []

    async def records(spec: RunSpec) -> int:
        binding = spec.inputs.input_for("group")
        value = await spec.store_in.read_json(binding.key, at=binding.scope)
        seen.append((binding.type.render(), binding.depth, value))
        await spec.store_out.put_json("n", "x")
        return 0

    def build(name, config, signature=None):
        if name == "take":
            return RunnerCoro(coro=records)
        from cascade.runners.build import configure_ref_builder
        return configure_ref_builder()(name, config, signature)

    store = FileStore(FileConfig(root=str(tmp_path), scope=("r1", "main")))
    assert await DagRunner("main", plan, build).run(
        RunSpec(name="main", run_id="r1", instance_id="r1/main", store_out=store, inputs=InputBindings())
    ) == 0
    assert len(seen) == 3
    for rendered, depth, value in seen:
        assert (rendered, depth) == ("string[]", 1)
        assert isinstance(value, list)
