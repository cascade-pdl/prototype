"""Port encodings: declared on a ref, persisted in the signature, delivered in the binding.

The store holds canonical JSON for everything structured, so an encoding says only what a
particular *container* wants on its local disk. Three consequences are pinned here: it
reaches the node, it plays no part in type compatibility, and it is meaningless on a dag
port because a dag transcodes nothing.
"""
import json

from yaml import safe_load

from cascade.types import TypeExpr
from cascade.model.pipeline import Pipeline
from cascade.types import DataFormat, IoConfig
from cascade.plan.compile import check, compile_pipeline
from cascade.plan.plan import Plan
from cascade.plan.signature import Port, Signature
from cascade.protocol.binding import InputBindings
from cascade.engine.resolve import resolve_node
from cascade.protocol.run_spec import RunSpec, to_env


PIPELINE = """
entrypoint: main
input: []
types:
  structures:
    - name: Detection
      fields: [ {name: x, type: float} ]
refs:
  - name: load
    runner: echo
    config: {}
    input: []
    output: [ { name: dets, type: "Detection[]", config: { encoding: %OUT% } } ]
  - name: score
    runner: echo
    config: {}
    input:  [ { name: dets,   type: "Detection[]", config: { encoding: %IN% } } ]
    output: [ { name: scored, type: "Detection[]" } ]
dags:
  - name: main
    input: [ { name: seed, type: "string"%DAG% } ]
    nodes:
      - { name: l, runs: load }
      - { name: s, runs: score, depends_on: [ { node: l, field: dets, as: dets } ] }
    output: [ { node: s, field: scored, as: out } ]
"""


def _pipeline(out="csv", in_="csv", dag=""):
    y = PIPELINE.replace("%OUT%", out).replace("%IN%", in_).replace("%DAG%", dag)
    return Pipeline.decode(safe_load(y))


# --- Port ---------------------------------------------------------------------

def test_a_port_defaults_to_json():
    assert Port(TypeExpr.parse("float")).encoding is DataFormat.json


def test_port_round_trips():
    port = Port(TypeExpr.parse("Detection[]"), IoConfig(DataFormat.csv))
    assert Port.decode(port.encode()) == port


def test_encoding_is_not_part_of_type_compatibility():
    """Two ports differing only in encoding are compatible: the store is canonical JSON
    either way, so encoding is presentation, not contract."""
    a = Port(TypeExpr.parse("Detection[]"), IoConfig(DataFormat.csv))
    b = Port(TypeExpr.parse("Detection[]"), IoConfig(DataFormat.json))
    assert a.type.accepts(b.type)
    assert b.type.accepts(a.type)


# --- persisted in the signature ----------------------------------------------

def test_a_refs_declared_encoding_reaches_its_signature():
    plan = compile_pipeline(_pipeline())
    sig = plan.signatures["score"]
    assert sig.inputs["dets"].encoding is DataFormat.csv
    assert sig.outputs["scored"].encoding is DataFormat.json  # undeclared -> canonical


def test_encodings_survive_the_plan_artifact():
    plan = compile_pipeline(_pipeline())
    back = Plan.decode(plan.encode())
    assert back == plan
    assert back.signatures["score"].inputs["dets"].encoding is DataFormat.csv


def test_a_dag_port_is_always_canonical():
    """A dag transcodes nothing, so its ports are JSON — which is precisely why there is
    no encoding to inherit along a dag's output alias."""
    plan = compile_pipeline(_pipeline())
    assert plan.signatures["main"].outputs["out"].encoding is DataFormat.json


# --- delivered in the binding -------------------------------------------------

def test_the_binding_carries_the_consuming_ports_encoding():
    plan = compile_pipeline(_pipeline(in_="csv"))
    graph = plan.node_graphs["main"]
    binding = resolve_node(graph.node("s"), plan, graph, InputBindings()).input_for("dets")
    assert binding.encoding is DataFormat.csv
    assert binding.depth == 1  # Detection[] -- what lets a node check what arrived


def test_the_encoding_crosses_into_the_container():
    plan = compile_pipeline(_pipeline(in_="csv"))
    graph = plan.node_graphs["main"]
    bindings = resolve_node(graph.node("s"), plan, graph, InputBindings())
    env = to_env(RunSpec(name="score", run_id="r1", inputs=bindings))
    (wire,) = json.loads(env["CASCADE_INPUTS"])
    # config travels whole, so mapping and a future transform arrive without plumbing
    assert wire["config"]["encoding"] == "csv"
    assert wire["type"] == "Detection[]"


def test_a_json_port_still_says_so_explicitly():
    """Explicit beats absent: a node should not have to guess a default."""
    plan = compile_pipeline(_pipeline(in_="json"))
    graph = plan.node_graphs["main"]
    binding = resolve_node(graph.node("s"), plan, graph, InputBindings()).input_for("dets")
    assert binding.encoding is DataFormat.json


# --- the dag-port rule --------------------------------------------------------

def test_an_encoding_on_a_dag_port_is_rejected():
    errors = check(_pipeline(dag=", config: { encoding: csv }"))
    assert any("meaningless on a dag port" in e for e in errors)


def test_json_on_a_dag_port_is_fine():
    assert check(_pipeline(dag=", config: { encoding: json }")) == []


def test_signature_can_be_built_directly_from_ports():
    """The shape the registry and tests construct by hand."""
    sig = Signature(
        inputs={"a": Port(TypeExpr.parse("float"))},
        outputs={"b": Port(TypeExpr.parse("float[]"), IoConfig(DataFormat.csv))},
    )
    assert Signature.decode(sig.encode()) == sig