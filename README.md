# cascade — authoring layer

Setting up a checkout, building the bundle, and running the examples in docker against a
local S3 store: see `DEVELOPMENT.md`.

Loads a pipeline, compiles it to a `Plan`, and validates it. The `Plan` is the
authoring → execution boundary: it carries everything the executor needs and
nothing it does not (no `Pipeline`, no passes, no `graphlib`).

## Layout

    cascade/
      graph.py            generic Graph[N,E] + serialization — neutral, dual-consumed
      model/              loaded pipeline data (decode + encode)
      plan/
        signature.py      TypeExpr, Signature          (artifact: derived I/O)
        type_env.py       TypeEnv + resolve_types       (artifact: runtime types, exec-only)
        plan.py           Plan                          (the crossing bundle; JSON round-trips)
        build.py          pipeline -> graphs            (pass; structural validation)
        elaborate.py      graphs -> signatures          (pass; shape + arity)
        validate.py       edges -> errors               (pass; type-name + arity)
        compile.py        pipeline -> Plan              (orchestrator + check())

Dependency direction is strictly downward: `engine -> plan -> model -> graph`.
The artifact modules (signature, type_env, plan) never import the passes, so an
executor importing `Plan` pulls in data definitions only.

## Use

    import yaml
    from cascade.model.pipeline import Pipeline
    from cascade.plan.compile import compile_pipeline, check

    pipe = Pipeline.decode(yaml.safe_load(open("examples/pipeline.yaml")))
    errors = check(pipe)          # [] if valid
    plan = compile_pipeline(pipe) # raises CompileError on any problem

    wire = json.dumps(plan.encode())          # ship to the executor
    plan = Plan.decode(json.loads(wire))      # rebuild the far side

## The bundle: cascade inside images that don't have it

A ref's image needs no cascade inside. `bundle/build.sh` builds a self-contained
`/opt/cascade` (a musl CPython that carries its own loader, so it runs in glibc, musl
and `FROM scratch` images alike) into `dist/bundle/`. Inside a container,

    /opt/cascade/bin/cascade node wrap -- <the image's own command...>

stages the node's inputs under `/cascade/inputs`, runs the command, and on success
collects `/cascade/outputs` into the store and marks the node done. The command's exit
code is passed through; stop signals are forwarded to it.

    bundle/build.sh
    CASCADE_BUNDLE=dist/bundle/opt-cascade poetry run pytest -q tests/cli/test_bundle.py

A docker ref is wrapped by default: the local backend mounts the bundle (its `bundle:`
setting) and runs the image's own entrypoint and command under it. A ref's `command:`
replaces the image's default command. `wrap: false` is for images that speak the
protocol themselves (the cascade SDK installed in them). `subprocess` refs are wrapped
the same way on the host, staging under `$CASCADE_ROOT`.

## Examples

Each directory under `examples/` is a project you can `cascade run` from inside it, and
is run by the test suite (`tests/test_mock_pipeline.py` for `mock`, the others by
`tests/test_examples.py`; those needing Docker or a built bundle are skipped without
them).

- `mock`: host scripts written against the node SDK (`subprocess`, `wrap: false`).
- `docker-raw`: a stock `python:3.13-slim` image with no cascade inside, wrapped by the
  bundle (`bundle/build.sh` first).
- `docker-sdk`: an image cascade builds from `refs/numbers/`, with the SDK installed from
  this checkout and an `entrypoint.py`; not wrapped.
- `docker-built-wrapped`: an image cascade builds from `refs/jq/` (Alpine + `jq`, no
  cascade), wrapped by the bundle.
- `limits`: host scripts (SDK, `wrap: false`): fanned arithmetic from one ref with the
  operation in `args`, then a 3 s `bulky-task` held to two at a time by a named limit
  (`limit: bulky` on the ref, `limits: {bulky: 2}` at the top of the pipeline).

## Images cascade builds

A docker ref names an existing image (`image:`) or a directory with a Dockerfile
(`build: refs/<name>/`, relative to the pipeline). Compile hashes the directory, and the
image is named `cascade/<dir>:<hash>`: it is built when that name does not exist yet
(`cascade run` does it before starting, `cascade build` on its own), and a changed
directory is a new name. No lock file. A Dockerfile that installs cascade copies it from
the named build context `cascade` (`COPY --from=cascade . /src/cascade`), which cascade
fills from this checkout; cascade's source then counts towards the hash. Pin `FROM`
images by digest: a moving tag can change under the same hash.

## S3 stores and credentials in containers

A deployment's `store:` may be S3 or an S3-compatible service (garage, MinIO), named by
`endpoint`; omit it for AWS itself:

    store:
      kind: s3
      config: { bucket: cascade, region: garage, endpoint: http://localhost:3900 }

Local docker containers run on the docker network `cascade` (`169.254.170.0/24`, created
when missing). With an S3 store, the run serves the credentials it uses itself (env,
profile or SSO) on the network's gateway, `169.254.170.2`, and containers fetch them
there as an ECS task fetches its role (`AWS_CONTAINER_CREDENTIALS_FULL_URI` plus a
per-run token): no keys in the image, nothing mounted, refreshed for long tasks. An
endpoint on the host's loopback is reached by containers at the gateway, so a local
service is published there as well as on loopback (`-p 127.0.0.1:3900:3900 -p
169.254.170.2:3900:3900`).

For development, `cascade dev garage up` runs garage that way with fixed values (port
3900, bucket `cascade`, region `garage`; data in the volume `cascade-garage`, keys under
`~/.local/state/cascade/garage`), so the deployment pointing at it is a constant: the
docker examples commit theirs as `deployment.garage.yaml`.

    cascade dev garage up
    eval "$(cascade dev garage env)"      # its keys become this shell's AWS identity
    cascade run pipeline.yaml --deployment deployment.garage.yaml
    cascade dev garage down [--purge]

Not a compose file: the network must exist before garage can publish on its gateway,
and garage's layout, bucket and key are made through its own CLI.

## Not yet built (deliberate)

- Type *identity* validation is name + arity only — exact match, no subtyping.
  `TypeEnv.is_subtype` exists for the runtime payload validator, not for edges.
- The executor (`engine/executor.py`): consume a `Plan`, run `waves()` per dag,
  dispatch to runners. Nested/black-box execution; scatter fan-width is runtime.
- `encode` exists on the model types the Plan carries (DagNode, Dependency, the
  type structures); other model types decode only.
