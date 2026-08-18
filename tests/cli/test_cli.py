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
from cascade.deployment import Deployment
from cascade.model.pipeline import Pipeline
from cascade.plan.compile import compile_pipeline
from cascade.project import Project, find_project


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
    assert raw["version"] == 5
    assert raw["dag_outputs"], "compiled plan lost dag_outputs"


def test_compile_default_output_path(project):
    assert main(["compile", str(project / "pipeline.yaml")]) == 0
    assert (project / "pipeline.plan.json").is_file()


def test_show_summarises_dag_outputs(plan_file, capsys):
    assert main(["show", str(plan_file)]) == 0
    out = capsys.readouterr().out
    assert "entrypoint: main" in out
    assert "each.s" in out
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


def test_new_creates_the_three_project_files(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    assert main(["new", "demo", "--yes"]) == 0
    root = tmp_path / "demo"
    assert (root / "cascade.toml").is_file()
    assert (root / "pipeline.yaml").is_file()
    assert (root / "deployment.yaml").is_file()


def test_scaffolded_files_load_through_their_real_loaders(tmp_path, monkeypatch, capsys):
    """The templates are hand-written text, so their loadability is asserted, not
    assumed."""
    monkeypatch.chdir(tmp_path)
    assert main(["new", "demo", "--yes"]) == 0
    root = tmp_path / "demo"
 
    project, resolved = find_project(root)
    assert project.name == "demo"
    assert resolved == root
 
    deployment = Deployment.load(project.deployment_file(root))
    assert deployment.store.root == "./_store"
 
    pipeline = Pipeline.decode(safe_load((root / "pipeline.yaml").read_text()))
    assert pipeline.entrypoint == "main"
    assert pipeline.refs == [] and pipeline.dags == []


 
def test_new_records_metadata_when_supplied(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    assert main(["new", "demo", "--description", "Moth work",
                 "--email", "l@example.com", "--yes"]) == 0
    project = Project.load(tmp_path / "demo" / "cascade.toml")
    assert project.description == "Moth work"
    assert project.email == "l@example.com"


def test_unsupplied_metadata_is_omitted_not_empty(tmp_path, monkeypatch, capsys):
    """TOML has no null, so unset fields must be absent from the file."""
    monkeypatch.chdir(tmp_path)
    assert main(["new", "demo", "--yes"]) == 0
    text = (tmp_path / "demo" / "cascade.toml").read_text()
    assert "description" not in text
    assert "email" not in text
    assert Project.load(tmp_path / "demo" / "cascade.toml").description is None


def test_new_accepts_an_explicit_target_path(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    target = tmp_path / "nested" / "here"
    assert main(["new", "demo", str(target), "--yes"]) == 0
    assert (target / "cascade.toml").is_file()

 
def test_new_accepts_an_existing_empty_directory(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    target = tmp_path / "empty"
    target.mkdir()
    assert main(["new", "demo", str(target), "--yes"]) == 0
    assert (target / "cascade.toml").is_file()
 
 
def test_new_refuses_a_non_empty_directory(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    target = tmp_path / "occupied"
    target.mkdir()
    (target / "keep.txt").write_text("mine")
    assert main(["new", "demo", str(target), "--yes"]) == 2
    assert "not empty" in capsys.readouterr().err
    assert (target / "keep.txt").read_text() == "mine"  # nothing clobbered

 
def test_new_refuses_a_file_target(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    target = tmp_path / "afile"
    target.write_text("x")
    assert main(["new", "demo", str(target), "--yes"]) == 2
    assert "not a directory" in capsys.readouterr().err

 
def test_new_rejects_an_empty_name(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    assert main(["new", "  ", "--yes"]) == 2
    assert "must not be empty" in capsys.readouterr().err

 
def test_store_commands_work_against_a_fresh_scaffold(tmp_path, monkeypatch, capsys):
    """The scaffolded deployment is immediately usable — no editing required."""
    monkeypatch.chdir(tmp_path)
    assert main(["new", "demo", "--yes"]) == 0
    capsys.readouterr()
 
    root = tmp_path / "demo"
    monkeypatch.chdir(root)
    src = tmp_path / "sample.txt"
    src.write_text("hello")
 
    assert main(["store", "stage", "sample.txt", str(src)]) == 0
    capsys.readouterr()
    assert main(["store", "list"]) == 0
    assert "sample.txt" in capsys.readouterr().out

 
def test_scaffolded_pipeline_validates_with_an_actionable_finding(tmp_path, monkeypatch, capsys):
    """A skeleton is intentionally incomplete: validate should say so clearly
    (exit 1, a finding — not a crash)."""
    monkeypatch.chdir(tmp_path)
    assert main(["new", "demo", "--yes"]) == 0
    capsys.readouterr()
    assert main(["validate", str(tmp_path / "demo" / "pipeline.yaml")]) == 1
    assert "entrypoint" in capsys.readouterr().out

# --- run ---------------------------------------------------------------------

ECHO_PIPELINE = """
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
dags:
  - name: main
    input: [ { name: src, type: string } ]
    nodes:
      - name: load
        runs: load
        depends_on: [ { node: "$input", field: src, as: src } ]
    output: [ { node: load, field: items, as: items } ]
"""


@pytest.fixture
def echo_project(project):
    """The scaffolded project, with an echo-only pipeline: `run` then needs no docker."""
    (project / "pipeline.yaml").write_text(ECHO_PIPELINE)
    return project


def test_run_executes_a_pipeline_in_a_project(echo_project, monkeypatch, capsys):
    """The whole loop: scaffolded project, deployment-resolved store, real execution."""
    monkeypatch.chdir(echo_project)
    assert main(["run", "pipeline.yaml", "--input", 'src="moths.jpg"', "--run-id", "r1"]) == 0
    out = capsys.readouterr().out
    assert "run r1 complete" in out
    assert "items" in out  # the declared output port
    # under the deployment's base scope: the substrate decides where, not the run
    run = echo_project / "_store" / "wilder" / "moth" / "r1"
    assert (run / "plan").is_file()
    assert (run / "main" / "load" / "items").is_file()
    assert (run / "main" / "$in" / "src").is_file()


def test_run_accepts_a_compiled_plan(echo_project, monkeypatch, capsys):
    monkeypatch.chdir(echo_project)
    assert main(["compile", "pipeline.yaml", "-o", "p.json"]) == 0
    capsys.readouterr()
    assert main(["run", "p.json", "--input", 'src="x"', "--run-id", "r2"]) == 0
    assert "run r2 complete" in capsys.readouterr().out


def test_run_reports_a_missing_input(echo_project, monkeypatch, capsys):
    monkeypatch.chdir(echo_project)
    assert main(["run", "pipeline.yaml", "--run-id", "r3"]) == 2
    assert "missing input" in capsys.readouterr().err


def test_run_rejects_a_malformed_input_flag(echo_project, monkeypatch, capsys):
    monkeypatch.chdir(echo_project)
    assert main(["run", "pipeline.yaml", "--input", "nonsense"]) == 2
    assert "name=json-value" in capsys.readouterr().err


def test_run_outside_a_project_is_a_hard_error(
    echo_project, tmp_path_factory, monkeypatch, capsys
):
    """Isolated dir, not one nested inside the project -- the walk would find it."""
    elsewhere = tmp_path_factory.mktemp("elsewhere")
    monkeypatch.chdir(elsewhere)
    assert main(["run", str(echo_project / "pipeline.yaml"), "--input", 'src="x"']) == 2
    assert "not inside a cascade project" in capsys.readouterr().err