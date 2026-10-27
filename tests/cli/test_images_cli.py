"""``cascade images``: which built images are still what their directory builds, and
removing the rest. Docker is stood in for: images are listed from a fixture and removals
recorded; the statuses come from hashing real directories."""
import shutil
import subprocess

import pytest

import cascade.cli.images as images_cli
import cascade.runners.images as runner_images
from cascade.cli.main import main
from cascade.docker.images import LocalImage
from cascade.plan.build_context import context_hash
from cascade.runners.images import LABEL_CONTEXT, LABEL_HASH, built_images


def _context(root, name: str):
    d = root / "refs" / name
    d.mkdir(parents=True)
    (d / "Dockerfile").write_text(f"FROM scratch\nLABEL name={name}\n")
    return d.resolve()


def _image(name: str, context=None, built_hash=None) -> LocalImage:
    labels = {LABEL_CONTEXT: str(context), LABEL_HASH: built_hash} if context else {}
    return LocalImage(id=f"sha256:{name}", tags=(f"cascade/{name}",), labels=labels)


@pytest.fixture
def world(tmp_path, monkeypatch):
    """Two projects. ``mine`` has a current, a stale and a gone image; ``theirs`` a
    stale one; and one image predates the labels."""
    mine, theirs = tmp_path / "mine", tmp_path / "theirs"
    for root in (mine, theirs):
        root.mkdir()
        (root / "cascade.toml").write_text(
            f'[project]\nname = "{root.name}"\npipeline = "p.yaml"\ndeployment = "d.yaml"\n'
        )
    current = _context(mine, "current")
    stale = _context(mine, "stale")
    stale_hash = context_hash(stale)
    (stale / "Dockerfile").write_text("FROM scratch\nLABEL changed=yes\n")
    gone = _context(mine, "gone")
    gone_hash = context_hash(gone)
    (gone / "Dockerfile").unlink()
    gone.rmdir()
    theirs_stale = _context(theirs, "stale")
    theirs_hash = context_hash(theirs_stale)
    (theirs_stale / "Dockerfile").write_text("FROM scratch\nLABEL changed=yes\n")

    listed = [
        _image("current:1", current, context_hash(current)),
        _image("stale:1", stale, stale_hash),
        _image("gone:1", gone, gone_hash),
        _image("theirs:1", theirs_stale, theirs_hash),
        _image("old:1"),
    ]
    removed = []

    async def list_images(reference):
        assert reference == "cascade/*"
        return listed

    async def remove_image(name):
        removed.append(name)
        return None

    monkeypatch.setattr(runner_images, "list_images", list_images)
    monkeypatch.setattr(images_cli, "remove_image", remove_image)
    monkeypatch.chdir(mine)
    return removed


@pytest.mark.asyncio
async def test_an_image_is_judged_against_its_own_directory(world):
    status = {b.names[0]: b.status for b in await built_images()}
    assert status == {
        "cascade/current:1": "current",
        "cascade/stale:1": "stale",
        "cascade/gone:1": "gone",
        "cascade/theirs:1": "stale",
        "cascade/old:1": "unlabelled",
    }


def test_list_shows_this_projects_images(world, capsys):
    assert main(["images", "list"]) == 0
    out = capsys.readouterr().out
    assert "current     cascade/current:1" in out and "refs/current" in out
    assert "cascade/stale:1" in out and "cascade/gone:1" in out
    assert "cascade/theirs:1" not in out
    assert "1 image(s) built before labels" in out


def test_prune_removes_this_projects_stale_and_gone_images_only(world, capsys):
    assert main(["images", "prune", "--yes"]) == 0
    assert sorted(world) == ["cascade/gone:1", "cascade/stale:1"]


def test_prune_all_reaches_other_projects_but_never_a_current_image(world):
    assert main(["images", "prune", "--all", "--yes"]) == 0
    assert sorted(world) == ["cascade/gone:1", "cascade/stale:1", "cascade/theirs:1"]


def test_unlabelled_images_go_only_when_asked(world):
    assert main(["images", "prune", "--unlabelled", "--yes"]) == 0
    assert "cascade/old:1" in world
    assert "cascade/current:1" not in world


def test_prune_does_not_remove_without_a_yes_when_it_cannot_ask(world, monkeypatch, capsys):
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)
    assert main(["images", "prune"]) == 2
    assert "pass --yes" in capsys.readouterr().err
    assert world == []


def test_prune_asks_and_a_no_removes_nothing(world, monkeypatch, capsys):
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda prompt: "n")
    assert main(["images", "prune"]) == 0
    assert world == []
    assert "nothing removed" in capsys.readouterr().out


def test_an_image_docker_will_not_remove_is_reported(world, monkeypatch, capsys):
    async def in_use(name):
        return "conflict: image is being used by running container"

    monkeypatch.setattr(images_cli, "remove_image", in_use)
    assert main(["images", "prune", "--yes"]) == 1
    assert "kept cascade/stale:1: conflict" in capsys.readouterr().err


# --- against docker ------------------------------------------------------------


def _docker_works() -> bool:
    if shutil.which("docker") is None:
        return False
    return subprocess.run(["docker", "info"], capture_output=True).returncode == 0


REAL_PIPELINE = """
entrypoint: main
input: []
refs:
  - { name: t, runner: docker, config: { build: refs/tool/, wrap: false },
      resources: { cpu: 1, memory: 1GiB }, input: [], output: [ { name: o, type: string } ] }
dags:
  - { name: main, input: [], nodes: [ { name: t, runs: t } ], output: [ { node: t, field: o, as: o } ] }
"""


@pytest.mark.skipif(not _docker_works(), reason="needs docker")
def test_a_rebuilt_directory_leaves_a_stale_image_that_prune_removes(tmp_path, monkeypatch, capsys):
    """Built, changed, built again: the first image is stale, the second current, and
    prune removes exactly the first."""
    project = tmp_path / "proj"
    tool = project / "refs" / "tool"
    tool.mkdir(parents=True)
    (tool / "Dockerfile").write_text("FROM scratch\nCOPY f /f\n")
    (tool / "f").write_text("v1")
    (project / "pipeline.yaml").write_text(REAL_PIPELINE)
    (project / "deployment.yaml").write_text(
        "store: { kind: file, config: { root: ./_store } }\n"
        "backends: [ { name: local, kind: local, max: { cpu: 2, memory: 2GiB } } ]\n"
    )
    (project / "cascade.toml").write_text(
        '[project]\nname = "proj"\npipeline = "pipeline.yaml"\ndeployment = "deployment.yaml"\n'
    )
    monkeypatch.chdir(project)
    made = []
    try:
        assert main(["build", "pipeline.yaml"]) == 0
        made.append(capsys.readouterr().out.split()[1])
        (tool / "f").write_text("v2")
        assert main(["build", "pipeline.yaml"]) == 0
        made.append(capsys.readouterr().out.split()[1])

        assert main(["images", "list"]) == 0
        out = capsys.readouterr().out
        assert f"stale       {made[0]}" in out
        assert f"current     {made[1]}" in out

        assert main(["images", "prune", "--yes"]) == 0
        assert f"removed {made[0]}" in capsys.readouterr().out
        assert subprocess.run(["docker", "image", "inspect", made[0]], capture_output=True).returncode != 0
        assert subprocess.run(["docker", "image", "inspect", made[1]], capture_output=True).returncode == 0
    finally:
        for image in made:
            subprocess.run(["docker", "image", "rm", image], capture_output=True)
