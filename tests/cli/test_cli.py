"""``cascade.cli`` — command dispatch, project resolution, and exit codes.

The first test is the one that matters most historically: the CLI has gone missing
from the tree twice while ``pyproject.toml`` kept declaring
``cascade = "cascade.cli:main"``, so a `pip install` succeeded and the `cascade`
command then failed at runtime. Importing the entry point here makes that
regression a red bar.

Exit-code convention asserted below: **0** success, **1** a legitimate negative
finding (an invalid pipeline), **2** a usage or environment error.
"""
import json

import pytest
from yaml import safe_load

from cascade.cli.main import main
from cascade.model.pipeline import Pipeline
from cascade.plan.compile import compile_pipeline


def test_declared_entry_point_is_importable():
    # mirrors pyproject.toml's `cascade = "cascade.cli:main"`
    from cascade.cli import main as exported

    assert callable(exported)


@pytest.fixture
def project(tmp_path, pipeline_str):
    """A complete project tree: cascade.toml + deployment.yaml + pipeline.yaml."""
    (tmp_path / "cascade.toml").write_text(
        '[project]\nname = "wilder-moth"\n'
        'pipeline = "pipeline.yaml"\ndeployment = "deployment.yaml"\n'
    )
    (tmp_path / "deployment.yaml").write_text(
        "name: local\n"
        "store:\n  kind: file\n  config:\n"
        f"    root: {tmp_path / '_store'}\n"
        "    scope: [wilder, moth]\n"
    )
    (tmp_path / "pipeline.yaml").write_text(pipeline_str)
    nested = tmp_path / "src" / "deep"
    nested.mkdir(parents=True)
    return tmp_path


@pytest.fixture
def plan_file(tmp_path, pipeline_str):
    plan = compile_pipeline(Pipeline.decode(safe_load(pipeline_str)))
    path = tmp_path / "main.plan.json"
    path.write_text(json.dumps(plan.encode()))
    return path


# --- author commands (pure; no project needed) -------------------------------

def test_validate_accepts_a_good_pipeline(project, capsys):
    assert main(["validate", str(project / "pipeline.yaml")]) == 0
    assert "valid" in capsys.readouterr().out


def test_validate_reports_findings_with_exit_1(tmp_path, capsys):
    bad = tmp_path / "bad.yaml"
    bad.write_text(
        "entrypoint: main\ninput: []\n"
        "refs:\n  - name: r\n    runner: echo\n    config: {}\n"
        "    input: []\n    output: [ { name: o, type: Nonexistent } ]\n"
        "dags:\n  - name: main\n    input: []\n"
        "    nodes: [ { name: n, runs: r } ]\n    output: []\n"
    )
    assert main(["validate", str(bad)]) == 1
    assert "unknown type" in capsys.readouterr().out


def test_compile_writes_a_plan(project, tmp_path):
    out = tmp_path / "out.plan.json"
    assert main(["compile", str(project / "pipeline.yaml"), "-o", str(out)]) == 0
    raw = json.loads(out.read_text())
    assert raw["version"] == 2
    assert raw["dag_outputs"], "compiled plan lost dag_outputs"


def test_compile_default_output_path(project):
    assert main(["compile", str(project / "pipeline.yaml")]) == 0
    assert (project / "pipeline.plan.json").is_file()


def test_show_summarises_dag_outputs(plan_file, capsys):
    assert main(["show", str(plan_file)]) == 0
    out = capsys.readouterr().out
    assert "entrypoint: main" in out
    assert "each.s [gather]" in out
    assert "integrity: ok" in out


def test_show_rejects_a_stale_plan(plan_file, tmp_path, capsys):
    raw = json.loads(plan_file.read_text())
    raw["version"] = 1
    stale = tmp_path / "stale.plan.json"
    stale.write_text(json.dumps(raw))
    assert main(["show", str(stale)]) == 2
    assert "recompile" in capsys.readouterr().err


def test_find_orphans_on_a_clean_plan(plan_file, capsys):
    assert main(["find-orphans", str(plan_file)]) == 0
    assert "no orphans" in capsys.readouterr().out


def test_missing_input_file_is_a_clean_error(tmp_path, capsys):
    assert main(["validate", str(tmp_path / "nope.yaml")]) == 2
    assert "not found" in capsys.readouterr().err


# --- store commands (project-resolved) --------------------------------------

def test_store_roundtrip_via_project_discovery(project, monkeypatch, capsys):
    """Run from a nested subdir: the project must be discovered upward, and the
    deployment's base scope applied."""
    monkeypatch.chdir(project / "src" / "deep")
    src = project / "a.txt"
    src.write_text("image-a")

    assert main(["store", "stage", "input.txt", str(src)]) == 0
    capsys.readouterr()

    assert main(["store", "list"]) == 0
    assert "input.txt" in capsys.readouterr().out

    dst = project / "got.txt"
    assert main(["store", "fetch", "input.txt", str(dst)]) == 0
    assert dst.read_text() == "image-a"

    # the deployment's scope decided the on-disk location
    assert (project / "_store" / "wilder" / "moth" / "input.txt").is_file()


def test_store_stage_dir_with_at_fragments(project, monkeypatch, capsys):
    monkeypatch.chdir(project)
    data = project / "imgs"
    data.mkdir()
    (data / "a.txt").write_text("a")
    (data / "b.txt").write_text("b")

    assert main(["store", "stage-dir", str(data), "--at", "raw"]) == 0
    capsys.readouterr()
    assert main(["store", "list", "--at", "raw"]) == 0
    listed = capsys.readouterr().out
    assert "a.txt" in listed and "b.txt" in listed


def test_flags_override_the_deployment(project, monkeypatch, capsys):
    """--backend builds the store from flags; the deployment is not consulted."""
    monkeypatch.chdir(project)
    src = project / "b.txt"
    src.write_text("flagged")
    alt = project / "flagstore"

    assert main(["store", "stage", "k.txt", str(src),
                 "--backend", "file", "--root", str(alt), "--scope", "alt"]) == 0
    assert (alt / "alt" / "k.txt").is_file()
    # nothing landed in the deployment's store
    assert not (project / "_store" / "wilder" / "moth" / "k.txt").exists()


def test_no_project_and_no_flags_is_a_hard_error(tmp_path, monkeypatch, capsys):
    """No silent fallback to a default store location."""
    empty = tmp_path / "empty"
    empty.mkdir()
    monkeypatch.chdir(empty)
    assert main(["store", "list"]) == 2
    assert "not inside a cascade project" in capsys.readouterr().err


def test_backend_flags_are_validated(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    assert main(["store", "list", "--backend", "file"]) == 2
    assert "requires --root" in capsys.readouterr().err
    assert main(["store", "list", "--backend", "s3"]) == 2
    assert "requires --bucket" in capsys.readouterr().err


def test_fetch_missing_key_is_a_clean_error(project, monkeypatch, capsys):
    monkeypatch.chdir(project)
    assert main(["store", "fetch", "absent", str(project / "x")]) == 2
    assert "not found" in capsys.readouterr().err