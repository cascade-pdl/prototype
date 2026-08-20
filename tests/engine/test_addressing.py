"""``cascade.engine.instance_path`` and ``cascade.engine.binding`` — the addressing types.

The property worth pinning is that an instance's path is *one* fact with two
renderings: a tuple for ``StoreConfig.subscope`` and a string for logs.
"""
import pytest

from cascade.types import TypeExpr
from cascade.engine.instance_path import InstancePath
from cascade.protocol.binding import InputBinding, InputBindings, OutputDecl
from cascade.types import DataFormat, IoConfig


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
            InputBinding(port="dets", scope=("d",), key="dets", type=TypeExpr.parse("Detection[]"), config=IoConfig(DataFormat.csv)),
            InputBinding(port="cfg", scope=("$input",), key="cfg", type=TypeExpr.parse("string")),
        ),
    )
    assert bindings.ports == ("dets", "cfg")
    assert bindings.input_for("dets").scope == ("d",)
    assert bindings.input_for("dets").encoding is DataFormat.csv
    assert bindings.input_for("cfg").encoding is DataFormat.json  # default
    assert bindings.input_for("absent") is None


def test_bindings_round_trip_as_json():
    bindings = InputBindings(
        inputs=(InputBinding(port="d", scope=("$input",), key="dets", type=TypeExpr.parse("Detection"), config=IoConfig(DataFormat.csv)),),
    )
    assert InputBindings.decode(bindings.encode()) == bindings


def test_empty_bindings_round_trip():
    assert InputBindings.decode(InputBindings().encode()) == InputBindings()
    assert InputBindings.decode([]) == InputBindings()


def test_encoded_bindings_are_json_serialisable():
    """They cross into containers via env, so they must survive json.dumps."""
    import json

    bindings = InputBindings(inputs=(InputBinding(port="p", scope=("d",), key="k", type=TypeExpr.parse("string")),))
    assert InputBindings.decode(json.loads(json.dumps(bindings.encode()))) == bindings


def test_bindings_carry_no_output_location():
    """An input needs a location because it points outside its own slot; an output does
    not, because `store_out` is already scoped to that slot. An earlier version paired
    them in one class with an `output_scope`, which held two incompatible frames of
    reference — so this pins that it stays gone."""
    binding = InputBinding(port="p", scope=(), key="k", type=TypeExpr.parse("string"))
    assert not hasattr(binding, "output_scope")
    assert not hasattr(OutputDecl(port="p", type=TypeExpr.parse("string")), "scope")
    assert not hasattr(OutputDecl(port="p", type=TypeExpr.parse("string")), "key")