"""``cascade.engine.run_spec`` — the spec and its env projection.

The env projection is the container wire format, so the tests assert on the exact
variables emitted and on the reader/writer store asymmetry that makes containment
structural.
"""
import json

from cascade.engine.run_spec import RunSpec, to_env
from cascade.engine.binding import InputBinding, InputBindings
from cascade.store.file_store import FileStore, FileConfig
from cascade.store.registry import decode as decode_store


def _store(tmp_path, *scope):
    return FileStore(FileConfig(root=str(tmp_path), scope=scope))


def test_identity_is_always_emitted():
    env = to_env(RunSpec(name="detect", run_id="r1", node_id="d", instance_id="r1/main/d"))
    spec = json.loads(env["CASCADE_RUN_SPEC"])
    assert spec == {
        "name": "detect",
        "run_id": "r1",
        "node_id": "d",
        "instance_id": "r1/main/d",
    }


def test_absent_optionals_emit_nothing():
    env = to_env(RunSpec(name="n", run_id="r"))
    assert set(env) == {"CASCADE_RUN_SPEC"}


def test_the_two_stores_are_emitted_separately(tmp_path):
    """Reader is dag-scoped so siblings are visible; writer is instance-scoped so
    the instance cannot write outside its slot."""
    env = to_env(
        RunSpec(
            name="n",
            run_id="r1",
            store_in=_store(tmp_path, "r1", "main"),
            store_out=_store(tmp_path, "r1", "main", "a"),
        )
    )
    reader = decode_store(json.loads(env["CASCADE_STORE_IN"]))
    writer = decode_store(json.loads(env["CASCADE_STORE_OUT"]))
    assert reader.scope == ("r1", "main")
    assert writer.scope == ("r1", "main", "a")


def test_inputs_are_emitted_and_round_trip():
    bindings = InputBindings(inputs=(InputBinding(port="dets", scope=("d",), key="dets"),))
    env = to_env(RunSpec(name="n", run_id="r", inputs=bindings))
    assert InputBindings.decode(json.loads(env["CASCADE_INPUTS"])) == bindings


def test_args_are_emitted():
    env = to_env(RunSpec(name="n", run_id="r", args={"threshold": 0.4, "flag": True}))
    assert json.loads(env["CASCADE_ARGS"]) == {"threshold": 0.4, "flag": True}


def test_empty_args_emit_nothing():
    assert "CASCADE_ARGS" not in to_env(RunSpec(name="n", run_id="r", args={}))


def test_caller_env_is_preserved_and_extended():
    env = to_env(RunSpec(name="n", run_id="r", env={"MY_VAR": "x"}))
    assert env["MY_VAR"] == "x"
    assert "CASCADE_RUN_SPEC" in env


def test_every_value_is_a_string(tmp_path):
    """Env vars must be strings — a dict or int leaking through would fail at spawn."""
    env = to_env(
        RunSpec(
            name="n",
            run_id="r",
            store_in=_store(tmp_path, "r"),
            store_out=_store(tmp_path, "r", "a"),
            inputs=InputBindings(inputs=(InputBinding(port="p", scope=("d",), key="k"),)),
            args={"n": 1},
        )
    )
    assert all(isinstance(v, str) for v in env.values())