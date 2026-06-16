# Cascade (Python reference implementation)

A declarative, container-native pipeline tool with **typed contracts between
steps**. You declare what runs (pre-built container images, each with a typed
input/output contract) and how the pieces connect (a DAG). Cascade checks that
every connection type-checks *before* anything runs — so when a model changes
its output format, you find out at `cascade validate`, not at 2am in production.

This is the minimal reference implementation: pure Python, refs are pre-built
images (push yours to ECR/GHCR and reference them), and the data plane is a
swappable key store (a local directory now, S3 for distribution).

## The idea in one screenful

```yaml
pipeline:
  types:
    structures:
      - name: Detection
        fields: [{name: detection_id, type: "string<uuid>"}, {name: confidence, type: float}]
      - name: MothCrop
        extends: Detection          # structural subtyping
        fields: [{name: is_moth, type: bool}]

  refs:
    - name: flat-bug
      image: 123.dkr.ecr.eu-west-1.amazonaws.com/flat-bug:v3   # pre-built
      runner: ecs-task
      input:  [{name: images, type: "Image[]"}]
      output: [{name: detections, type: "Detection[]"}]

  dag:
    detect:
      ref: flat-bug
      depends_on: [{node: load, field: images, as: "--images"}]
    classify-moth:
      ref: moth-classifier
      scatter: detections           # fan out: one instance per detection
      depends_on: [{node: detect, field: detections, as: "--detection"}]
```

## The magic moment

```
$ cascade validate examples/moth-pipeline.yaml
validating refs...
  refs: ok
validating dag connections...
  [ERROR] node 'track' <- 'classify-species': type mismatch: upstream provides
          'SpeciesGuess[]' (after gather) but downstream expects 'ClassifiedMoth[]'
  dags: FAILED
```

A model changed its output type; the connection no longer type-checks; you know
exactly which edge broke and why — before running anything.

## Commands

```
cascade validate <pipeline.yaml>   # check refs + dag connections (the type check)
cascade graph    <pipeline.yaml>   # print the resolved execution waves
cascade run      <pipeline.yaml> --input frames=<key> --store DIR [--dry-run]
cascade query    <run_id> --store DIR [--node NODE [--instance INST]]
```

## How it's organised (the protocol boundaries, as modules)

| Module        | Role |
|---------------|------|
| `model.py`    | the data model (the YAML, parsed) |
| `loader.py`   | YAML → model |
| `types.py`    | type expressions, structural subtyping, edge transforms — **the core idea** |
| `validate.py` | `validate_dags` (the type check) + a thin `validate_refs` |
| `plan.py`     | flatten subdags + topological sort → execution plan |
| `store.py`    | the data plane: `put`/`get`/`has` by key (filesystem now, S3 stub) |
| `hooks.py`    | node-side translation: codecs (json/csv) + type-preserving field rename |
| `runner.py`   | launch a container; `HookedRunner` wraps it with the data-plane hooks; ECS stub; echo for dry runs |
| `engine.py`   | the coordinator: walk waves, thread keys, **expand scatter at runtime**, build per-instance port plans, record run state |
| `cli.py`      | the `cascade` command |

## Encoding and field mapping

The data plane always stores the **canonical encoding (JSON) with canonical
field names** — so connecting nodes is N+M, not N×M. Each ref declares how *its
container* differs from canonical, and the node-side hooks translate at the
boundary. The container stays oblivious to canonical names and formats.

```yaml
refs:
  - name: flat-bug
    image: .../flat-bug:v3
    encoding: csv                 # this container reads/writes CSV locally
    output:
      - name: detections
        type: "Detection[]"
        mapping: { detection_id: det_id }   # container's det_id <-> canonical detection_id
```

- `encoding` (ref-level, or per-port override) — the container's local format
  (`json` | `csv`; add codecs in `hooks.py`). The store stays JSON; the hook
  re-encodes to/from the container's format.
- `mapping` (per-port) — a **type-preserving field rename** between canonical
  names and the container's local names. It may *only* relabel — never compute,
  reshape, or convert units (those are nodes, not hooks). The line:
  **representation changes (rename, re-encode) live in the hook; value
  computation lives in a node.**

This is what lets a team migrate existing mixed-format glue: each node keeps its
own format and field names locally; only the invisible intermediate in the
store is canonical, and nothing in the container code changes.

## Key design properties (carried from the protocol design)

- **Pre-built images as refs.** A ref is `image:` + a typed contract. No build
  system — you build images by hand and push them. (Build-from-source is a
  future `build:` variant.)
- **Data plane / control plane split.** Nodes read/write payloads from the
  store by key, inside the container. The runner and engine only move *pointers*
  (keys) and read *metadata* — never payloads. This is what keeps the runner
  dumb and lets the same pipeline run local (`docker run`) or distributed (ECS,
  100-way fan-out) by swapping the runner.
- **Runtime scatter.** The plan marks scatter *points*; the engine resolves the
  *count* at runtime from the upstream node's reported `item_keys`. Scatter
  carries through downstream single-mode edges until a `gather` collapses it.
- **Run state is the result catalog.** Retrieve any node/instance output by
  `(run_id, node_id, instance_key)` via `cascade query`.

## The container entrypoint contract

Build your images to read pointers from env and do their own I/O:

```
CASCADE_INPUT_KEYS    JSON {binding: store_key}   — read your inputs from these
CASCADE_OUTPUT_PREFIX where to write output(s)
CASCADE_MANIFEST_KEY  where to write your metadata blob:
                      {"output_key": ..., "output_cardinality": N, "item_keys": [...]}
CASCADE_ARGS          JSON of the node's static args
```

`item_keys` is what lets the engine fan out a downstream scatter — a node that
produces a collection reports one key per element.

## Run the demo

```
pip install -e .
cascade validate examples/moth-pipeline.yaml
cascade graph    examples/moth-pipeline.yaml
python demo_run.py          # full end-to-end with a mock runner (no Docker)
python -m pytest -q         # tests
```

## Status & next steps

This runs and is tested (types, subtyping, validation incl. drift, planning,
scatter execution). Clearly-marked extension points, in rough priority:

- **EcsRunner** (`runner.py`) — wire up boto3 `run_task`/`describe_tasks` for
  scaled fan-out. The simplest high-value next piece.
- **S3Store** (`store.py`) — wire up boto3 for distributed data plane.
- **Subdag flattening** (`plan.py`) — `$input` rewiring + sink-node resolution
  for nested `dags:`. Single-level ref pipelines work today.
- **Runtime drift check** — pull the image, verify its actual contract matches
  the declaration (the `latest`-drift mechanism). Belongs in the run stage.
- **Sinks** — terminal-node output destinations (CSV/Dynamo) by service-config
  reference, with `whole`/`items` granularity.
