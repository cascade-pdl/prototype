"""Tests for ``cascade.project`` — the discovery walk is the part worth locking."""
import pytest

from cascade.project import (
    Project,
    find_project_file,
    find_project,
    ProjectNotFound,
    PROJECT_FILE,
)


TOML_TABLE = """
[project]
name = "wilder-moth"
pipeline = "pipe.yaml"
deployment = "deploy.yaml"
"""


def test_decode_accepts_project_table():
    import tomllib

    proj = Project.decode(tomllib.loads(TOML_TABLE))
    assert proj == Project(name="wilder-moth", pipeline="pipe.yaml", deployment="deploy.yaml")


def test_decode_accepts_flat_mapping():
    # a hand-edited toml without the [project] wrapper still decodes
    assert Project.decode({"name": "flat"}) == Project(name="flat")


def test_decode_applies_defaults():
    proj = Project.decode({"project": {"name": "only-name"}})
    assert proj.pipeline == "pipeline.yaml"
    assert proj.deployment == "deployment.yaml"


def test_encode_decode_round_trip():
    proj = Project(name="p", pipeline="a.yaml", deployment="b.yaml")
    assert Project.decode(proj.encode()) == proj


def _write_project(root):
    (root / PROJECT_FILE).write_text('[project]\nname = "wilder-moth"\n')


def test_find_project_file_walks_up_from_nested_dir(tmp_path):
    root = tmp_path / "proj"
    nested = root / "src" / "deep"
    nested.mkdir(parents=True)
    _write_project(root)
    assert find_project_file(nested) == root / PROJECT_FILE


def test_find_project_file_accepts_a_file_as_start(tmp_path):
    root = tmp_path / "proj"
    (root / "src").mkdir(parents=True)
    _write_project(root)
    a_file = root / "src" / "module.py"
    a_file.write_text("x = 1\n")
    assert find_project_file(a_file) == root / PROJECT_FILE


def test_find_project_file_raises_when_absent(tmp_path):
    empty = tmp_path / "nowhere"
    empty.mkdir()
    with pytest.raises(ProjectNotFound):
        find_project_file(empty)


def test_find_project_returns_project_and_root(tmp_path):
    root = tmp_path / "proj"
    root.mkdir()
    _write_project(root)
    proj, resolved_root = find_project(root)
    assert proj.name == "wilder-moth"
    assert resolved_root == root
    assert proj.pipeline_file(resolved_root) == root / "pipeline.yaml"
    assert proj.deployment_file(resolved_root) == root / "deployment.yaml"


def test_dump_round_trips_through_load(tmp_path):
    project = Project(name="p", description="desc", email="e@example.com")
    path = tmp_path / PROJECT_FILE
    project.save(path)
    assert Project.load(path) == project


def test_dump_omits_unset_optionals(tmp_path):
    # TOML has no null: absent is the only way to spell "unset"
    text = Project(name="p").dump()
    assert "description" not in text and "email" not in text


def test_dump_escapes_quotes_in_values(tmp_path):
    project = Project(name='odd "quoted" name')
    path = tmp_path / PROJECT_FILE
    project.save(path)
    assert Project.load(path).name == 'odd "quoted" name'
