from cascade.model.pipeline import Pipeline
from yaml import safe_load

pipeline_raw = """
entrypoint: main

# Type structures: encoded and shipped to the executor env alongside the store
# config, so each node can resolve a port's declared type, deserialise the payload
# (per the port's encoding), validate it against the structure, and post it to the
# data plane via the Store. 'io.Image' is an opaque/binary built-in, not a record,
# so it is intentionally not declared here.
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
        - { name: bbox,       type: "BBox" }          # nested structure reference
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
      - { name: s, type: "Score", config: { encoding: "csv" } }   # encoding tells the node how to read bytes before validating against Score

dags:
  - name: analyse
    input: [ { name: dets, type: "Detection[]" } ]
    nodes:
      - name: each
        ref: score
        scatter: d
        depends_on: [ { node: "$input", field: dets, as: d } ]
    output: [ { node: each, field: s, as: scores, mode: gather } ]
  - name: main
    input: [ { name: image, type: "io.Image" } ]
    nodes:
      - name: d
        ref: detect
        depends_on: [ { node: "$input", field: image, as: image } ]
      - name: a
        ref: analyse
        depends_on: [ { node: d, field: dets, as: dets } ]
    output: [ { node: a, field: scores, as: scores } ]
"""


def test_pipeline_decode():
    pipe = Pipeline.decode(safe_load(pipeline_raw))
    assert pipe.dags
    assert pipe.refs
    assert pipe.types
