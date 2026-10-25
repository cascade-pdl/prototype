"""Ref configs: ``command`` and ``wrap``, and refusing what would silently run otherwise."""
import pytest

from cascade.model.ref_data import RefDocker, RefSubprocess


def test_a_docker_ref_is_wrapped_and_runs_the_image_default_unless_told_otherwise():
    ref = RefDocker.decode({"image": "python:3.13-slim"})
    assert ref.wrap is True
    assert ref.command is None


def test_a_docker_ref_takes_a_command_and_can_opt_out_of_wrapping():
    ref = RefDocker.decode({"image": "x:1", "command": ["tool", "-v"], "wrap": False})
    assert ref.command == ["tool", "-v"]
    assert ref.wrap is False
    assert RefDocker.decode(ref.encode()) == ref


@pytest.mark.parametrize("command", ["tool -v", [], ["tool", 3], {"a": 1}])
def test_a_command_must_be_a_non_empty_list_of_strings(command):
    """A string would be split by nobody: ``"tool -v"`` would run a binary called
    ``tool -v``. Argv form only, as Docker's exec form and ECS take it."""
    with pytest.raises(ValueError, match="`command` must be a non-empty list of strings"):
        RefDocker.decode({"image": "x:1", "command": command})


@pytest.mark.parametrize("wrap", ["no", 0, None])
def test_wrap_must_be_a_boolean(wrap):
    with pytest.raises(ValueError, match="`wrap` must be true or false"):
        RefDocker.decode({"image": "x:1", "wrap": wrap})


def test_a_misspelt_setting_is_refused_not_dropped():
    with pytest.raises(ValueError, match=r"unknown field\(s\) \['wrapp'\]"):
        RefDocker.decode({"image": "x:1", "wrapp": False})


def test_a_docker_ref_needs_an_image():
    with pytest.raises(ValueError, match="`image` must be a non-empty string"):
        RefDocker.decode({"command": ["tool"]})


def test_a_subprocess_ref_is_wrapped_unless_told_otherwise():
    assert RefSubprocess.decode({"cmd": ["tool"]}).wrap is True
    ref = RefSubprocess.decode({"cmd": ["python", "x.py"], "wrap": False})
    assert ref.wrap is False
    assert RefSubprocess.decode(ref.encode()) == ref


def test_a_subprocess_command_is_checked_like_a_docker_one():
    with pytest.raises(ValueError, match="`cmd` must be a non-empty list of strings"):
        RefSubprocess.decode({"cmd": "python x.py"})
    with pytest.raises(ValueError, match="unknown field"):
        RefSubprocess.decode({"cmd": ["x"], "command": ["y"]})
