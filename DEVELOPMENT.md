# Development

From a fresh checkout to running pipelines in docker against a local S3 store. For what
cascade is and how the code is laid out, see `README.md`.

Tested on Linux (x86_64). Docker Desktop on macOS/Windows is untested: the container
network relies on the host owning the bridge's gateway address.

## 1. Prerequisites

- Python >= 3.13 and [poetry](https://python-poetry.org/) 2.x
- Docker 23 or later (BuildKit, for `--build-context`), usable without `sudo`
- For the bundle build: `curl`, `sha256sum`, and `strip` / `readelf` (binutils)

## 2. Install and check

    poetry install
    poetry run pytest -q
    poetry run ruff check cascade tests

Every command below assumes cascade is on your `PATH`: either activate the venv
(`eval $(poetry env activate)`) or prefix each command with `poetry run`, including the
one inside `eval "$(...)"` in section 6.

Tests that need Docker or a built bundle skip themselves when those are missing, so a
first run shows some skips. After sections 3 and 6 they all run.

## 3. Build the bundle

Docker refs are wrapped by default: cascade mounts a self-contained `/opt/cascade` into
the container and runs the image's own command under it, so images need no cascade
inside. Build it once:

    bundle/build.sh

It downloads a pinned musl CPython, installs cascade from this checkout into it, and
writes `dist/bundle/opt-cascade/` (about 60 MB; the examples point at it) plus a
reproducible archive.

**Rebuild it whenever you change cascade code that runs inside containers** (the node
SDK, `cascade node wrap`, the stores, the protocol). The bundle carries its own copy of
cascade; a stale one fails inside the container, typically with a decode error on a
field it does not know.

The bundle's own tests are opt-in:

    CASCADE_BUNDLE=dist/bundle/opt-cascade poetry run pytest -q tests/cli/test_bundle.py

## 4. Run an example

Each directory under `examples/` is a project: `cd` into it and `cascade run`. Results go
to `./_store` (a file store) unless the deployment says otherwise.

    cd examples/mock
    cascade run pipeline.yaml          # host scripts, no docker needed

| example | runs | needs |
|---|---|---|
| `mock` | host scripts using the node SDK | the venv (scripts import cascade) |
| `limits` | host scripts, a named concurrency limit | the venv |
| `docker-raw` | stock `python:3.13-slim`, wrapped by the bundle | docker, the bundle |
| `docker-sdk` | an image cascade builds from `refs/numbers/`, cascade inside, not wrapped | docker |
| `docker-built-wrapped` | an image cascade builds from `refs/jq/`, wrapped | docker, the bundle |

A ref with `build: refs/<name>/` gets its image built on the first `cascade run`, named
by the directory's content hash; change the directory and the next run builds a new
one. `cascade build pipeline.yaml` builds ahead of time.

Useful while iterating:

    cascade placement pipeline.yaml --explain   # which backend each ref lands on, and why
    cascade store list                          # what the run wrote

## 5. How docker refs reach the store

Local docker containers run on the docker network `cascade` (`169.254.170.0/24`), which
`cascade run` creates the first time it is needed. Its gateway, `169.254.170.2`, is the
host as containers see it:

- **credentials**: with an S3 store, `cascade run` serves the AWS identity of *your
  shell* there, for the length of the run, and containers fetch it the way an ECS task
  fetches its role. Nothing is mounted and no keys go in images or deployment files.
- **a store on your machine**: an endpoint on `localhost` in the deployment is rewritten
  to the gateway inside containers.

So: whatever AWS identity your shell has is what the run uses, on the host and in every
container.

## 6. A local S3 store: garage

`cascade dev garage` runs [garage](https://garagehq.deuxfleurs.fr/) in docker with fixed
values (port 3900, bucket `cascade`, region `garage`), so a deployment pointing at it is a
constant. The docker examples commit one as `deployment.garage.yaml`.

    cd examples/docker-built-wrapped
    cascade dev garage up                    # network, garage, bucket and key; keeps what exists
    eval "$(cascade dev garage env)"         # garage's key becomes this shell's AWS identity
    cascade run pipeline.yaml --deployment deployment.garage.yaml
    cascade store list --deployment deployment.garage.yaml
    cascade dev garage down                  # data and keys kept; --purge removes them

- The `eval` affects the current shell only; repeat it in a new one. Without it the run
  either stops before starting ("no AWS credentials") or garage rejects your real AWS
  identity at the first write.
- Data lives in the docker volume `cascade-garage`; config and keys in
  `~/.local/state/cascade/garage` (`$XDG_STATE_HOME`).
- Garage is published on `127.0.0.1:3900` and `169.254.170.2:3900` only, not on your
  LAN.
- It is not a compose file: the network must exist before garage can be published on
  its gateway, and garage's bucket and key are made through its own CLI.

The test suite starts its own throwaway garage (own name, free port, no volume), so it
never touches this one.

## 7. Troubleshooting

- **`docker network 'cascade' exists with subnet/gateway ...`** or **`subnet
  169.254.170.0/24 is taken`**: another network uses the range (AWS's
  `amazon-ecs-local-container-endpoints` setup does). Remove it, or the `cascade`
  network, and run again.
- **`garage up` fails to publish port 3900**: something else listens there. Stop it;
  the port is fixed on purpose.
- **A wrapped run fails inside the container with a traceback from `/opt/cascade`**:
  rebuild the bundle (section 3).
- **`... has changed since the plan was compiled`**: a `build:` directory changed after
  compiling a `.json` plan; compile again, or run the YAML.
- **Image not present, `no_pull` stops it being pulled**: the backend has `no_pull: true`
  (the default); pull the image yourself or set `no_pull: false`.

## Conventions

- Run the suite and ruff before committing; new behaviour comes with a test.
- Layering is checked by `tests/engine/test_layering.py`: a new top-level package under
  `cascade/` must be ranked there, and imports only go downward.
