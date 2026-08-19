"""The node-side SDK (6.1b) and the hook commands (6.1a).

Everything here is exercised through the real env contract — the same variables
``to_env`` produces — because that contract *is* the protocol, and the SDK is only one
implementation of it. If a test reached inside instead, it would be testing the
implementation rather than the thing a Rust ref would have to reimplement.
"""
import json

import pytest

from cascade.engine.binding import (
    InputBinding,
    InputBindings,
    OutputDecl,
    OutputDecls,
)
from cascade.engine.run_spec import RunSpec, to_env
from cascade.model.types import DataFormat, TypeExpr
from cascade.node.node import DONE_MARKER
from cascade.node import Node, NodeError, from_env, session
from cascade.node.codec import CodecError, decode, encode
from cascade.store.file_store import FileConfig, FileStore


@pytest.fixture
def stores(tmp_path):
    base = FileConfig(root=str(tmp_path / "store"), scope=("r1", "main"))
    return FileStore(base), FileStore(base.subscope(("detect",)))


@pytest.fixture
def spec(stores):
    reader, writer = stores
    reader.put_json("images", [{"id": i} for i in range(3)], at=("list",))
    return RunSpec(
        name="detect",
        run_id="r1",
        node_id="detect",
        instance_id="r1/main/detect",
        store_in=reader,
        store_out=writer,
        inputs=InputBindings(
            (InputBinding("images", ("list",), "images", TypeExpr.parse("Image[]")),)
        ),
        outputs=OutputDecls((OutputDecl("dets", TypeExpr.parse("Detection[]")),)),
        args={"threshold": 0.4},
    )


def _env(spec: RunSpec, tmp_path) -> dict[str, str]:
    """The mapping a runner would set. A function as well as a fixture, because a test
    that mutates ``spec`` has to build it *after* doing so — pytest resolves fixtures
    before the body runs."""
    return {**to_env(spec), "CASCADE_ROOT": str(tmp_path / "cascade")}


@pytest.fixture
def env(spec, tmp_path) -> dict[str, str]:
    """Exactly what a runner would set, as a plain mapping.

    ``from_env`` takes the mapping explicitly, so a test needs no monkeypatching: the
    contract is a pure function of its input and the global environment stays untouched.
    """
    return _env(spec, tmp_path)


# --- the env contract ---------------------------------------------------------

def test_a_node_is_built_entirely_from_the_environment(env):
    n = from_env(env)
    assert (n.name, n.run_id, n.node_id, n.instance_id) == (
        "detect", "r1", "detect", "r1/main/detect",
    )
    assert n.args == {"threshold": 0.4}
    assert n.inputs.ports == ("images",)
    assert n.outputs.ports == ("dets",)


def test_an_empty_environment_gives_an_inert_node_not_a_broken_one():
    """No patching required to test the empty case, which is the point of passing the
    mapping in."""
    n = from_env({})
    assert n.inputs.ports == () and n.store_in is None
    with pytest.raises(NodeError, match="no input bound"):
        n.read("anything")


# --- reading ------------------------------------------------------------------

def test_read_returns_the_canonical_value(env):
    n = from_env(env)
    assert n.read("images") == [{"id": 0}, {"id": 1}, {"id": 2}]


def test_read_resolves_a_distributed_collection(spec, stores, tmp_path):
    """Whether the upstream materialised its collection is invisible to a ref."""
    reader, _ = stores
    # element scopes are relative to the descriptor's own location, which is ("list",)
    for i in range(2):
        reader.put_json("e", {"id": i}, at=("list", "fan", str(i)))
    reader.write_collection("images", [(("fan", "0"), "e"), (("fan", "1"), "e")], at=("list",))
    n = from_env(_env(spec, tmp_path))
    assert n.read("images") == [{"id": 0}, {"id": 1}]


def test_an_unbound_port_names_what_is_bound(env):
    n = from_env(env)
    with pytest.raises(NodeError, match=r"no input bound for port 'nope'.*images"):
        n.read("nope")


def test_a_shape_mismatch_fails_at_the_node(spec, stores, tmp_path):
    """The declared depth is what turns a silent misread into an error."""
    reader, _ = stores
    reader.put_json("images", {"not": "a list"}, at=("list",))
    n = from_env(_env(spec, tmp_path))
    with pytest.raises(NodeError, match="declared 1-dimensional"):
        n.read("images")


# --- staging: the file and directory idioms -----------------------------------

def test_dir_stages_a_collection_as_one_file_per_element(env):
    """The idiom batch tools need: `fb_predict -i <dir>`."""
    n = from_env(env)
    staged = n.dir("images")
    files = sorted(p.name for p in staged.iterdir())
    assert files == ["0.json", "1.json", "2.json"]
    assert json.loads((staged / "1.json").read_text()) == {"id": 1}


def test_dir_zero_pads_so_filename_order_is_element_order(spec, stores, tmp_path):
    """A directory carries no order, and a tool that sorts its inputs would otherwise
    scramble the correspondence the gather depends on: '10' sorts before '2'."""
    reader, _ = stores
    reader.put_json("images", [{"id": i} for i in range(11)], at=("list",))
    n = from_env(_env(spec, tmp_path))
    names = sorted(p.name for p in n.dir("images").iterdir())
    assert names[:2] == ["00.json", "01.json"]
    assert names[-1] == "10.json"


def test_path_stages_a_single_value(spec, stores, tmp_path):
    reader, _ = stores
    reader.put_json("images", {"only": "one"}, at=("list",))
    spec.inputs = InputBindings(
        (InputBinding("images", ("list",), "images", TypeExpr.parse("Image")),)
    )
    n = from_env(_env(spec, tmp_path))
    assert json.loads(n.path("images").read_text()) == {"only": "one"}


def test_dir_refuses_a_scalar_port(spec, stores, tmp_path):
    reader, _ = stores
    reader.put_json("images", {"only": "one"}, at=("list",))
    spec.inputs = InputBindings(
        (InputBinding("images", ("list",), "images", TypeExpr.parse("Image")),)
    )
    n = from_env(_env(spec, tmp_path))
    with pytest.raises(NodeError, match="not a collection"):
        n.dir("images")


# --- writing ------------------------------------------------------------------

def test_write_stores_a_canonical_value(stores, env):
    _, writer = stores
    n = from_env(env)
    n.write("dets", [{"conf": 0.9}])
    assert writer.read_json("dets") == [{"conf": 0.9}]


def test_write_dir_collects_a_directory_in_filename_order(stores, env):
    _, writer = stores
    n = from_env(env)
    out = n.tempdir()
    for i in range(3):
        (out / f"{i}.json").write_text(json.dumps({"i": i}))
    n.write_dir("dets", out)
    assert writer.read_json("dets") == [{"i": 0}, {"i": 1}, {"i": 2}]


def test_write_collection_uses_the_distributed_form(stores, env):
    """Nothing is concatenated, so large elements cost the same as small ones."""
    _, writer = stores
    n = from_env(env)
    n.write_collection("dets", [{"i": 0}, {"i": 1}])
    assert writer.is_collection("dets")
    assert writer.read_json("dets") == [{"i": 0}, {"i": 1}]


def test_writing_an_undeclared_port_is_refused(env):
    n = from_env(env)
    with pytest.raises(NodeError, match="not a declared output port"):
        n.write("surprise", [])


def test_an_empty_output_directory_is_an_error(env):
    n = from_env(env)
    with pytest.raises(NodeError, match="no files matching"):
        n.write_dir("dets", n.tempdir())


# --- the completion marker ----------------------------------------------------

def test_a_clean_exit_writes_the_marker_last(stores, env):
    _, writer = stores
    n = from_env(env)
    with n:
        n.write("dets", [])
    assert writer.get_json(DONE_MARKER) == {
        "instance": "r1/main/detect",
        "ports": ["dets"],
    }


def test_a_failing_ref_leaves_no_marker(stores, env):
    """Outputs may exist and still not be complete — which is the whole point of a
    marker written last."""
    _, writer = stores
    n = from_env(env)
    with pytest.raises(RuntimeError):
        with n:
            n.write("dets", [])
            raise RuntimeError("model crashed")
    assert writer.has("dets")
    assert not writer.has(DONE_MARKER)


def test_forgetting_a_declared_port_fails_rather_than_marking_done(stores, env):
    _, writer = stores
    n = from_env(env)
    with pytest.raises(NodeError, match="never written"):
        with n:
            pass
    assert not writer.has(DONE_MARKER)


def test_the_scratch_directory_is_cleaned_up(env):
    n = from_env(env)
    with n:
        scratch = n.tempdir()
        assert scratch.is_dir()
        n.write("dets", [])
    assert not scratch.exists()


# --- codec --------------------------------------------------------------------

def test_json_round_trips():
    value = [{"a": 1, "b": "x"}]
    assert decode(encode(value, DataFormat.json), DataFormat.json) == value


def test_csv_round_trips_flat_records():
    value = [{"conf": 0.9, "cls": 1}, {"conf": 0.4, "cls": 2}]
    assert decode(encode(value, DataFormat.csv), DataFormat.csv) == value


def test_csv_refuses_nested_values():
    """A Detection with a nested bbox, or flat-bug's float[][] contour, is not tabular —
    which is why flat-bug's port must be json."""
    with pytest.raises(CodecError, match="nested"):
        encode([{"bbox": {"x": 1}}], DataFormat.csv)


def test_csv_refuses_a_scalar():
    with pytest.raises(CodecError, match="list of records"):
        encode({"not": "a list"}, DataFormat.csv)


def test_an_empty_csv_round_trips_to_an_empty_list():
    assert decode(encode([], DataFormat.csv), DataFormat.csv) == []

# --- session ------------------------------------------------------------------

def test_session_returns_a_context_manager_not_a_node(env):
    """The point of `session` over `from_env`: `n = session(env)` cannot be mistaken for
    a node, so the lifecycle cannot be skipped by accident."""
    handle = session(env)
    assert not isinstance(handle, Node)
    assert hasattr(handle, "__enter__")


def test_session_writes_the_marker_on_a_clean_exit(stores, env):
    _, writer = stores
    with session(env) as n:
        n.write("dets", [{"conf": 0.9}])
    assert writer.has(DONE_MARKER)


def test_session_skips_the_marker_when_the_body_raises(stores, env):
    """Exception semantics are delegated to `Node`, not reimplemented: the error is
    thrown in at the yield and propagates out of `with node`."""
    _, writer = stores
    with pytest.raises(RuntimeError, match="crashed"):
        with session(env) as n:
            n.write("dets", [])
            raise RuntimeError("model crashed")
    assert writer.has("dets")
    assert not writer.has(DONE_MARKER)


def test_session_accepts_os_environ(spec, tmp_path):
    """`os.environ` is an `os._Environ`, not a dict — which is why the signature is
    Mapping. Every production call site passes exactly this type."""
    import os

    assert isinstance(from_env(os.environ), Node)