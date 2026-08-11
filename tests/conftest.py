import pytest
from yaml import safe_load

from cascade.model.pipeline import Pipeline
from cascade.plan.plan import Plan
from cascade.plan.compile import compile_pipeline


PIPELINE = """
entrypoint: main

types:
  structures:
    - name: BBox
      fields:
        - { name: x, type: "float" }
        - { name: y, type: "float" }
        - { name: w, type: "float" }
        - { name: h, type: "float" }
    - name: Detection
      fields:
        - { name: bbox,       type: "BBox" }
        - { name: label,      type: "string" }
        - { name: confidence, type: "float" }
    - name: Score
      extends: Detection                              # structural single-inheritance
      fields:
        - { name: species, type: "string" }
        - { name: score,   type: "float" }

input:
  - { name: image, type: "io.Image" }

refs:
  - name: detect
    runner: docker
    config: { image: "123.dkr.ecr.eu-west-1.amazonaws.com/flat-bug:v3" }
    input:  [ { name: image, type: "io.Image" } ]
    output: [ { name: dets,  type: "Detection[]" } ]
  - name: score
    runner: subprocess
    config: { cmd: ["python", "-m", "score"] }
    input:  [ { name: d, type: "Detection" } ]
    output:
      - { name: s, type: "Score", config: { encoding: "csv" } }

dags:
  - name: analyse
    input: [ { name: dets, type: "Detection[]" } ]
    nodes:
      - name: each
        runs: score
        scatter: d
        depends_on: [ { node: "$input", field: dets, as: d } ]
    output: [ { node: each, field: s, as: scores, mode: gather } ]
  - name: main
    input: [ { name: image, type: "io.Image" } ]
    nodes:
      - name: d
        runs: detect
        depends_on: [ { node: "$input", field: image, as: image } ]
      - name: a
        runs: analyse
        depends_on: [ { node: d, field: dets, as: dets } ]
    output: [ { node: a, field: scores, as: scores } ]
"""


@pytest.fixture
def pipeline_str() -> str:
    return str(PIPELINE)


@pytest.fixture
def pipeline(pipeline_str) -> Pipeline:
    return Pipeline.decode(safe_load(pipeline_str))


@pytest.fixture
def plan(pipeline: Pipeline) -> Plan:
    return compile_pipeline(pipeline=pipeline)
