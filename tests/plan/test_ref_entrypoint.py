"""A ref as the entrypoint: run one unit on its own.

The executor runs dags, so compile wraps a ref entrypoint in an implicit one-node dag
named ``main``: the ref's inputs become the dag's, its outputs the dag's. The store
layout is then the usual ``<run>/main/<ref>/...``.
"""
import json

import pytest
from yaml import safe_load

from cascade.cli import main
from cascade.model.pipeline import Pipeline
from cascade.plan.compile import CompileError, check, compile_pipeline

REFS = """
refs:
  - name: gen
    runner: subprocess
    resources: { cpu: 0.25, memory: 256MiB }
    config: { cmd: ["sh", "-c", "echo '[1, 2, 3]' > $CASCADE_ROOT/outputs/numbers"] }
    input: []
    output: [ { name: numbers, type: "int[]" } ]
  - name: add-one
    runner: subprocess
    resources: { cpu: 0.25, memory: 256MiB }
    config:
      cmd: ["sh", "-c", "echo $(( $(cat $CASCADE_ROOT/inputs/n) + 1 )) > $CASCADE_ROOT/outputs/m"]
    input:  [ { name: n, type: int } ]
    output: [ { name: m, type: int } ]
"""


def pipeline(entrypoint: str, dags: str = "dags: []") -> Pipeline:
    return Pipeline.decode(safe_load(f"entrypoint: {entrypoint}\ninput: []\n{REFS}\n{dags}"))


def test_a_ref_entrypoint_runs_as_an_implicit_main_dag():
    plan = compile_pipeline(pipeline("gen"))
    assert plan.entrypoint == "main"
    assert [(i, n.runnable_name) for i, n in plan.node_graphs["main"].nodes()] == [("gen", "gen")]
    assert list(plan.signatures["main"].outputs) == ["numbers"]
    assert list(plan.signatures["main"].inputs) == []


def test_the_implicit_dag_takes_the_ref_inputs():
    plan = compile_pipeline(pipeline("add-one"))
    assert list(plan.signatures["main"].inputs) == ["n"]
    assert list(plan.signatures["main"].outputs) == ["m"]


def test_a_ref_entrypoint_cannot_share_the_name_main():
    dags = """dags:
  - { name: main, input: [], nodes: [ { name: g, runs: gen } ],
      output: [ { node: g, field: numbers, as: numbers } ] }"""
    [error] = check(pipeline("add-one", dags))
    assert "'main'" in error and "add-one" in error
    with pytest.raises(CompileError):
        compile_pipeline(pipeline("add-one", dags))


def test_a_dag_entrypoint_is_untouched():
    dags = """dags:
  - { name: top, input: [], nodes: [ { name: g, runs: gen } ],
      output: [ { node: g, field: numbers, as: numbers } ] }"""
    plan = compile_pipeline(pipeline("top", dags))
    assert plan.entrypoint == "top"
    assert "main" not in plan.node_graphs


def test_a_ref_entrypoint_runs_end_to_end(tmp_path, monkeypatch, capsys):
    (tmp_path / "pipeline.yaml").write_text(f"entrypoint: add-one\ninput: []\n{REFS}\ndags: []\n")
    (tmp_path / "deployment.yaml").write_text(
        "store: { kind: file, config: { root: ./_store } }\n"
        "backends: [ { name: local, kind: local, max: { cpu: 2, memory: 2GiB } } ]\n")
    (tmp_path / "cascade.toml").write_text(
        '[project]\nname = "x"\npipeline = "pipeline.yaml"\ndeployment = "deployment.yaml"\n')
    monkeypatch.chdir(tmp_path)
    assert main(["run", "pipeline.yaml", "--input", "n=41", "--run-id", "r1"]) == 0
    assert "m: add-one/m" in capsys.readouterr().out
    stored = tmp_path / "_store" / "x" / "r1" / "main" / "add-one" / "m"
    assert json.loads(stored.read_text()) == 42
