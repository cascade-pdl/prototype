"""``DagRunner`` and binding resolution — milestone M1.

A two-node dag compiled from a pipeline, executed in process against ``MemoryStore``
with ``echo`` refs. What this proves is the whole addressing model: node ``score``
reads node ``load``'s output through a dag-scoped reader, with nothing staged and
nothing copied, and the store layout mirrors the dag.
"""
import pytest
from yaml import safe_load

from cascade.types import TypeExpr
from cascade.model.pipeline import Pipeline
from cascade.plan.compile import compile_pipeline
from cascade.protocol.binding import InputBinding, InputBindings
from cascade.engine.resolve import (
    ResolveError,
    resolve_dag_output,
    resolve_node,
)
from cascade.protocol.run_spec import RunSpec
from cascade.engine.runner.runner_dag import DagRunner, DagRunError
from cascade.store.file_store import FileConfig, FileStore


LINEAR = """
entrypoint: main
input:
  - { name: src, type: string }
types:
  structures:
    - name: Item
      fields: [ { name: k, type: string } ]
    - name: Scored
      fields: [ { name: s, type: float } ]
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
    output: [ { name: scored, type: Scored } ]
dags:
  - name: main
    input: [ { name: src, type: string } ]
    nodes:
      - name: load
        runs: load
        depends_on: [ { node: "$input", field: src,   as: src } ]
      - name: score
        runs: score
        depends_on: [ { node: load,     field: items, as: items } ]
    output: [ { node: score, field: scored, as: result } ]
"""


@pytest.fixture
def plan():
    return compile_pipeline(Pipeline.decode(safe_load(LINEAR)))


@pytest.fixture
def stores(tmp_path):
    """A run-scoped writer for the dag, matching what the executor will hand it."""
    base = FileConfig(root=str(tmp_path), scope=("wilder", "moth"))
    return FileStore(base.subscope(("r1", "main")))


# --- resolution --------------------------------------------------------------

def test_resolves_a_dag_input_edge(plan):
    graph = plan.node_graphs["main"]
    dag_inputs = InputBindings(inputs=(InputBinding(port="src", scope=("$in",), key="src", type=TypeExpr.parse("string")),))
    binding = resolve_node(graph.node("load"), plan, graph, dag_inputs).input_for("src")
    assert (binding.scope, binding.key) == (("$in",), "src")


def test_resolves_a_sibling_edge_to_the_producer_scope(plan):
    """Static: a node's output always lives at its own name, so no runtime result is
    consulted."""
    graph = plan.node_graphs["main"]
    binding = resolve_node(graph.node("score"), plan, graph, InputBindings()).input_for("items")
    assert (binding.scope, binding.key) == (("load",), "items")


def test_unbound_dag_input_is_an_error(plan):
    graph = plan.node_graphs["main"]
    with pytest.raises(ResolveError):
        resolve_node(graph.node("load"), plan, graph, InputBindings())


def test_a_fanned_producer_is_bound_like_any_other(plan):
    """A fan closes at its own node, so a consumer sees one collection at the
    producer's scope — the same binding it would get from an unfanned node."""
    graph = plan.node_graphs["main"]
    graph.node("load").scatter = "src"
    binding = resolve_node(graph.node("score"), plan, graph, InputBindings()).input_for("items")
    assert (binding.scope, binding.key) == (("load",), "items")


# --- dag output aliasing -----------------------------------------------------

def test_dag_output_resolves_to_the_inner_node_scope(plan):
    """The alias: derivable from the plan, so nothing is written or copied."""
    assert resolve_dag_output(plan, "main", "result") == (("score",), "scored")


def test_unknown_dag_output_port_is_an_error(plan):
    with pytest.raises(ResolveError):
        resolve_dag_output(plan, "main", "nope")


def test_output_scopes_reports_the_alias(plan):
    assert DagRunner("main", plan).output_scopes() == {"result": (("score",), "scored")}


# --- M1: the run -------------------------------------------------------------

def test_dag_runner_rejects_an_unknown_dag(plan):
    with pytest.raises(DagRunError):
        DagRunner("nosuchdag", plan)


@pytest.mark.asyncio
async def test_m1_two_node_dag_runs_end_to_end(plan, stores, tmp_path):
    # the dag's own input, as the executor will stage it
    stores.put_json("src", {"path": "moths.jpg"}, at=("$in",))
    dag_inputs = InputBindings(inputs=(InputBinding(port="src", scope=("$in",), key="src", type=TypeExpr.parse("string")),))

    code = await DagRunner("main", plan).run(
        RunSpec(
            name="main",
            run_id="r1",
            instance_id="r1/main",
            store_out=stores,
            inputs=dag_inputs,
        )
    )
    assert code == 0

    # both nodes wrote into their own slots, beneath the dag's
    assert stores.get_json("items", at=("load",))[0]["port"] == "items"  # Item[] -> list
    assert stores.get_json("scored", at=("score",))["port"] == "scored"

    # the layout mirrors the dag, under the deployment's base scope
    written = sorted(
        str(p.relative_to(tmp_path)).replace("\\", "/")
        for p in tmp_path.rglob("*")
        if p.is_file()
    )
    assert written == [
        "wilder/moth/r1/main/$in/src",
        "wilder/moth/r1/main/load/items",
        "wilder/moth/r1/main/score/scored",
    ]


@pytest.mark.asyncio
async def test_the_downstream_node_reads_the_upstream_output(plan, stores):
    """Nothing is staged: score's binding points into load's slot, and the bytes there
    are the ones load wrote."""
    stores.put_json("src", {"path": "x"}, at=("$in",))
    runner = DagRunner("main", plan)
    await runner.run(
        RunSpec(
            name="main", run_id="r1", instance_id="r1/main", store_out=stores,
            inputs=InputBindings(
                inputs=(InputBinding(port="src", scope=("$in",), key="src", type=TypeExpr.parse("string")),)
            ),
        )
    )
    graph = plan.node_graphs["main"]
    binding = resolve_node(graph.node("score"), plan, graph, InputBindings()).input_for("items")
    assert stores.get_json(binding.key, at=binding.scope)[0]["runnable"] == "load"


@pytest.mark.asyncio
async def test_instance_paths_nest_under_the_dag(plan, stores):
    """Each node's instance_id is the dag's path plus its own name."""
    seen = []
    stores.put_json("src", {}, at=("$in",))
    runner = DagRunner("main", plan)
    original = runner._runner_for

    def spy(runnable):
        inner = original(runnable)
        real_invoke = inner._invoke

        def wrapped(spec):
            seen.append(spec.instance_id)
            return real_invoke(spec)

        inner._invoke = wrapped
        return inner

    runner._runner_for = spy
    await runner.run(
        RunSpec(
            name="main", run_id="r1", instance_id="r1/main", store_out=stores,
            inputs=InputBindings(
                inputs=(InputBinding(port="src", scope=("$in",), key="src", type=TypeExpr.parse("string")),)
            ),
        )
    )
    assert sorted(seen) == ["r1/main/load", "r1/main/score"]


# --- M4: nested dags ---------------------------------------------------------

NESTED = """
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
  - name: tidy
    runner: echo
    config: {}
    input:  [ { name: items, type: "Item[]" } ]
    output: [ { name: clean, type: "Item[]" } ]
dags:
  - name: prep
    input: [ { name: raw, type: "Item[]" } ]
    nodes:
      - name: t
        runs: tidy
        depends_on: [ { node: "$input", field: raw, as: items } ]
    output: [ { node: t, field: clean, as: ready } ]
  - name: main
    input: [ { name: src, type: string } ]
    nodes:
      - name: l
        runs: load
        depends_on: [ { node: "$input", field: src, as: src } ]
      - name: p
        runs: prep
        depends_on: [ { node: l, field: items, as: raw } ]
    output: [ { node: p, field: ready, as: result } ]
"""


@pytest.fixture
def nested_plan():
    return compile_pipeline(Pipeline.decode(safe_load(NESTED)))


def test_alias_chases_through_a_subdag(nested_plan):
    """main exports prep's output, which exports its own inner node's — one chain,
    resolved from the plan, no artifact written at either level."""
    assert resolve_dag_output(nested_plan, "prep", "ready") == (("t",), "clean")
    assert resolve_dag_output(nested_plan, "main", "result") == (("p", "t"), "clean")


@pytest.mark.asyncio
async def test_m4_a_nested_dag_runs_by_recursion(nested_plan, tmp_path):
    store = FileStore(FileConfig(root=str(tmp_path), scope=("r1", "main")))
    store.put_json("src", {"p": "x"}, at=("$in",))

    code = await DagRunner("main", nested_plan).run(
        RunSpec(
            name="main", run_id="r1", instance_id="r1/main", store_out=store,
            inputs=InputBindings(
                inputs=(InputBinding(port="src", scope=("$in",), key="src", type=TypeExpr.parse("string")),)
            ),
        )
    )
    assert code == 0

    # the subdag's node wrote one level deeper, beneath its own dag node's slot
    written = sorted(
        str(p.relative_to(tmp_path)).replace("\\", "/")
        for p in tmp_path.rglob("*")
        if p.is_file()
    )
    assert written == [
        "r1/main/$in/src",
        "r1/main/l/items",
        "r1/main/p/t/clean",
    ]


@pytest.mark.asyncio
async def test_a_subdag_node_resolves_its_input_through_the_alias(nested_plan, tmp_path):
    """A consumer of a dag node binds to where the bytes actually are, not to the
    dag node's own scope."""
    store = FileStore(FileConfig(root=str(tmp_path), scope=("r1", "main")))
    store.put_json("src", {"p": "x"}, at=("$in",))
    await DagRunner("main", nested_plan).run(
        RunSpec(
            name="main", run_id="r1", instance_id="r1/main", store_out=store,
            inputs=InputBindings(
                inputs=(InputBinding(port="src", scope=("$in",), key="src", type=TypeExpr.parse("string")),)
            ),
        )
    )
    # what a hypothetical downstream of 'p' would be told
    scope, key = resolve_dag_output(nested_plan, "prep", "ready")
    assert store.get_json(key, at=("p", *scope))[0]["runnable"] == "tidy"