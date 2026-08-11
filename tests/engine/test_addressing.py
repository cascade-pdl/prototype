"""``cascade.engine.instance_path`` and ``cascade.engine.binding`` — the addressing types.

The property worth pinning is that an instance's path is *one* fact with two
renderings: a tuple for ``StoreConfig.subscope`` and a string for logs.
"""
import pytest

from cascade.engine.instance_path import InstancePath
from cascade.engine.binding import InputBinding, InputBindings
from cascade.model.types import DataFormat


# --- InstancePath ------------------------------------------------------------

def test_root_starts_at_the_run():
    path = InstancePath.root("r1")
    assert path.scope == ("r1",)
    assert str(path) == "r1"


def test_child_descends_by_name():
    path = InstancePath.root("r1").child("main").child("a")
    assert path.scope == ("r1", "main", "a")
    assert str(path) == "r1/main/a"


def test_child_accepts_several_segments_at_once():
    assert InstancePath.root("r1").child("main", "a").scope == ("r1", "main", "a")


def test_lane_descends_into_a_fan_index():
    path = InstancePath.root("r1").child("analyse", "each").lane(3)
    assert path.scope == ("r1", "analyse", "each", "3")
    assert str(path) == "r1/analyse/each/3"


def test_nesting_and_fanning_compose():
    """Two reasons to recurse, one path: a dag inside a lane inside a dag."""
    path = (
        InstancePath.root("r1")
        .child("main", "a")
        .child("analyse", "each")
        .lane(7)
    )
    assert str(path) == "r1/main/a/analyse/each/7"


def test_scope_is_the_store_subscope():
    path = InstancePath.root("r1").child("main", "d")
    assert path.scope == path.segments  # same fact, no translation


def test_parent_and_name():
    path = InstancePath.root("r1").child("main", "d")
    assert path.name == "d"
    assert path.parent.scope == ("r1", "main")


def test_paths_are_frozen_and_comparable():
    a = InstancePath.root("r1").child("main")
    b = InstancePath.root("r1").child("main")
    assert a == b
    with pytest.raises(Exception):
        a.segments = ("mutated",)


def test_child_rejects_segments_that_would_corrupt_the_scope():
    path = InstancePath.root("r1")
    with pytest.raises(ValueError):
        path.child("a/b")
    with pytest.raises(ValueError):
        path.child("")
    with pytest.raises(ValueError):
        path.lane(-1)


def test_descending_does_not_mutate_the_parent():
    parent = InstancePath.root("r1").child("main")
    parent.child("a")
    parent.lane(0)
    assert parent.scope == ("r1", "main")


# --- InputBindings -----------------------------------------------------------

def test_bindings_locate_inputs_in_sibling_scopes():
    bindings = InputBindings(
        inputs=(
            InputBinding(port="dets", scope=("d",), key="dets", encoding=DataFormat.csv),
            InputBinding(port="cfg", scope=("$input",), key="cfg"),
        ),
    )
    assert bindings.ports == ("dets", "cfg")
    assert bindings.input_for("dets").scope == ("d",)
    assert bindings.input_for("dets").encoding is DataFormat.csv
    assert bindings.input_for("cfg").encoding is DataFormat.json  # default
    assert bindings.input_for("absent") is None


def test_bindings_round_trip_as_json():
    bindings = InputBindings(
        inputs=(InputBinding(port="d", scope=("$input",), key="dets", encoding=DataFormat.csv),),
    )
    assert InputBindings.decode(bindings.encode()) == bindings


def test_empty_bindings_round_trip():
    assert InputBindings.decode(InputBindings().encode()) == InputBindings()
    assert InputBindings.decode([]) == InputBindings()


def test_encoded_bindings_are_json_serialisable():
    """They cross into containers via env, so they must survive json.dumps."""
    import json

    bindings = InputBindings(inputs=(InputBinding(port="p", scope=("d",), key="k"),))
    assert InputBindings.decode(json.loads(json.dumps(bindings.encode()))) == bindings


def test_bindings_carry_no_output_information():
    """Outputs are signature data, not per-instance data: every instance of a
    runnable writes the same ports, and where they land is fixed by store_out."""
    assert not hasattr(InputBindings(), "output_scope")
    assert not hasattr(InputBinding(port="p", scope=(), key="k"), "output_scope")