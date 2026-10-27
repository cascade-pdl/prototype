"""Preparing built images before a run, against a stand-in ``docker``.

The stand-in remembers which images exist and records each build's arguments, and what
the ``cascade`` named context held when it was given one.
"""
import os
import sys

import pytest
from yaml import safe_load

from cascade.cli import main
from cascade.executor import place
from cascade.model.pipeline import Pipeline
from cascade.model.resources import Resources
from cascade.plan.compile import compile_pipeline
from cascade.runners.images import ImageBuildError, prepare_images, resolve_images
from cascade.substrate.backends import (
    Backend,
    BackendKind,
    EcsConfig,
    LocalConfig,
    uncapped_local,
)
from cascade.substrate.placement import Placement, PlacementMap, place_refs

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="a shell script stands in for docker")

FAKE_DOCKER = r"""#!/bin/sh
state="$FAKE_DOCKER_STATE"
if [ "$1 $2" = "image inspect" ]; then
  echo "$3" >> "$state/inspects"
  if grep -qxF "$3" "$state/images" 2>/dev/null; then
    echo '[{"Id": "sha256:id-of-'"$3"'", "Config": {"Entrypoint": null, "Cmd": ["run"]}}]'
    exit 0
  fi
  echo "Error: No such image: $3" >&2; exit 1
fi
if [ "$1 $2" = "pull --quiet" ]; then
  echo "$3" >> "$state/pulls"; echo "$3" >> "$state/images"; exit 0
fi
if [ "$1" = build ]; then
  echo "$@" >> "$state/builds"
  tag=""; named=""
  while [ $# -gt 0 ]; do
    case "$1" in --tag) tag="$2"; shift ;; --build-context) named="${2#cascade=}"; shift ;; esac
    shift
  done
  [ -n "$named" ] && (cd "$named" && find . -type f | sort) > "$state/staged"
  if [ -f "$state/fail" ]; then echo "step 2/3 failed: no such package"; exit 3; fi
  echo "$tag" >> "$state/images"
  exit 0
fi
exit 2
"""


@pytest.fixture
def docker(tmp_path, monkeypatch):
    bin_dir, state = tmp_path / "bin", tmp_path / "state"
    bin_dir.mkdir()
    state.mkdir()
    (bin_dir / "docker").write_text(FAKE_DOCKER)
    (bin_dir / "docker").chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_DOCKER_STATE", str(state))
    return state


def builds(state) -> list[str]:
    return (state / "builds").read_text().splitlines() if (state / "builds").exists() else []


PIPELINE = """
entrypoint: main
input: []
refs:
  - { name: a, runner: docker, config: { build: refs/tool/, command: [a], wrap: false },
      resources: { cpu: 1, memory: 1GiB }, input: [], output: [ { name: o, type: string } ] }
  - { name: b, runner: docker, config: { build: refs/tool/, command: [b], wrap: false },
      resources: { cpu: 1, memory: 1GiB }, input: [], output: [ { name: o, type: string } ] }
dags:
  - name: main
    input: []
    nodes: [ { name: a, runs: a }, { name: b, runs: b } ]
    output: [ { node: a, field: o, as: o } ]
"""


@pytest.fixture
def project(tmp_path):
    d = tmp_path / "project"
    (d / "refs" / "tool").mkdir(parents=True)
    (d / "refs" / "tool" / "Dockerfile").write_text("FROM alpine@sha256:abc\n")
    (d / "pipeline.yaml").write_text(PIPELINE)
    return d


def compiled(project):
    plan = compile_pipeline(Pipeline.decode(safe_load(PIPELINE)), base=project)
    return plan, place(plan, None)


@pytest.mark.asyncio
async def test_refs_sharing_a_directory_share_one_build(docker, project):
    plan, placements = compiled(project)
    built = await prepare_images(plan, placements, [uncapped_local()], str(project))
    assert len(built) == 1 and built[0].startswith("cascade/tool:")
    assert len(builds(docker)) == 1


@pytest.mark.asyncio
async def test_an_existing_name_is_an_up_to_date_image(docker, project):
    plan, placements = compiled(project)
    await prepare_images(plan, placements, [uncapped_local()], str(project))
    assert await prepare_images(plan, placements, [uncapped_local()], str(project)) == []
    assert len(builds(docker)) == 1


@pytest.mark.asyncio
async def test_cascade_source_is_passed_only_to_a_dockerfile_that_uses_it(docker, project):
    plan, placements = compiled(project)
    await prepare_images(plan, placements, [uncapped_local()], str(project))
    assert "--build-context" not in builds(docker)[0]

    (project / "refs" / "tool" / "Dockerfile").write_text(
        "FROM python@sha256:abc\nCOPY --from=cascade . /src/cascade\n")
    plan, placements = compiled(project)
    await prepare_images(plan, placements, [uncapped_local()], str(project))
    assert "--build-context cascade=" in builds(docker)[1]
    staged = (docker / "staged").read_text().splitlines()
    assert {"./pyproject.toml", "./README.md", "./cascade/__init__.py"} <= set(staged)


@pytest.mark.asyncio
async def test_a_directory_changed_since_compile_is_refused(docker, project):
    plan, placements = compiled(project)
    (project / "refs" / "tool" / "extra.txt").write_text("new")
    with pytest.raises(ImageBuildError, match="has changed since the plan was compiled"):
        await prepare_images(plan, placements, [uncapped_local()], str(project))
    assert builds(docker) == []


@pytest.mark.asyncio
async def test_a_failed_build_says_so_with_dockers_last_words(docker, project):
    (docker / "fail").touch()
    plan, placements = compiled(project)
    with pytest.raises(ImageBuildError, match=r"(?s)failed \(docker exit 3\).*no such package"):
        await prepare_images(plan, placements, [uncapped_local()], str(project))


@pytest.mark.asyncio
async def test_only_local_backends_build_so_far(docker, project):
    plan, _ = compiled(project)
    ecs = Backend(name="ecs-cpu", kind=BackendKind.ecs, max=Resources(4, 8192),
                  config=EcsConfig(cluster="c", subnets=("s",)), concurrency=2)
    with pytest.raises(ImageBuildError, match="only local backends"):
        await prepare_images(plan, PlacementMap((Placement("a", "ecs-cpu"),)), [ecs], str(project))


def test_cascade_build_builds_then_reports_up_to_date(docker, project, monkeypatch, capsys):
    (project / "deployment.yaml").write_text(
        "store: { kind: file, config: { root: ./_store } }\n"
        "backends: [ { name: local, kind: local, max: { cpu: 2, memory: 2GiB } } ]\n")
    (project / "cascade.toml").write_text(
        '[project]\nname = "x"\npipeline = "pipeline.yaml"\ndeployment = "deployment.yaml"\n')
    monkeypatch.chdir(project)
    assert main(["build", "pipeline.yaml"]) == 0
    assert capsys.readouterr().out.startswith("built cascade/tool:")
    assert main(["build", "pipeline.yaml"]) == 0
    assert capsys.readouterr().out == "all images up to date\n"


# --- resolving each image once per run -----------------------------------------

def lines(state, name) -> list[str]:
    return (state / name).read_text().splitlines() if (state / name).exists() else []


@pytest.mark.asyncio
async def test_refs_sharing_an_image_are_resolved_with_one_inspection(docker, project):
    plan, placements = compiled(project)
    await prepare_images(plan, placements, [uncapped_local()], str(project))
    inspected_before = len(lines(docker, "inspects"))
    images = await resolve_images(plan, placements, [uncapped_local()])
    assert len(lines(docker, "inspects")) == inspected_before + 1
    assert images["a"] == images["b"]
    assert images["a"].id.startswith("sha256:id-of-cascade/tool:")
    assert images["a"].cmd == ("run",)


NAMED = """
entrypoint: main
input: []
refs:
  - { name: a, runner: docker, config: { image: "vendor/tool:1", wrap: false },
      resources: { cpu: 1, memory: 1GiB }, input: [], output: [ { name: o, type: string } ] }
dags:
  - { name: main, input: [], nodes: [ { name: a, runs: a } ], output: [ { node: a, field: o, as: o } ] }
"""


def local(**config):
    return Backend(name="local", kind=BackendKind.local, max=Resources(8, 16384),
                   config=LocalConfig(**config), provides=None)


@pytest.mark.asyncio
async def test_a_missing_image_is_pulled_once_when_the_backend_allows(docker):
    plan = compile_pipeline(Pipeline.decode(safe_load(NAMED)))
    backends = [local(no_pull=False)]
    images = await resolve_images(plan, place_refs(plan, backends), backends)
    assert lines(docker, "pulls") == ["vendor/tool:1"]
    assert images["a"].id == "sha256:id-of-vendor/tool:1"


@pytest.mark.asyncio
async def test_a_missing_image_the_backend_will_not_pull_names_its_refs(docker):
    plan = compile_pipeline(Pipeline.decode(safe_load(NAMED)))
    backends = [local(no_pull=True)]
    with pytest.raises(ImageBuildError, match=r"\['a'\].*not present locally"):
        await resolve_images(plan, place_refs(plan, backends), backends)
    assert lines(docker, "pulls") == []


@pytest.mark.asyncio
async def test_a_built_image_is_never_pulled(docker, project):
    """It exists only where it was built: a pull could only fetch something else."""
    plan, _ = compiled(project)
    backends = [local(no_pull=False)]
    with pytest.raises(ImageBuildError, match="not present locally"):
        await resolve_images(plan, place_refs(plan, backends), backends)  # not built yet
    assert lines(docker, "pulls") == []
