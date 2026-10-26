"""The examples in ``examples/``, run the way a user runs them: ``cascade run``.

Each example is documentation and an end-to-end test at once, so it is run as shipped,
from a copy of its directory, with only machine paths adjusted. Examples that need Docker
or a built bundle (``bundle/build.sh``) are skipped when those are absent.
"""
import shutil
import time
from pathlib import Path

import pytest
import yaml

from cascade.cli import main
from cascade.store.file_store import FileConfig, FileStore


def _repo_root() -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").is_file():
            return parent
    raise RuntimeError("no pyproject.toml above this test; cannot locate the repo root")


ROOT = _repo_root()
EXAMPLES = ROOT / "examples"
BUNDLE = ROOT / "dist" / "bundle" / "opt-cascade"

needs_docker = pytest.mark.skipif(
    shutil.which("docker") is None, reason="needs docker"
)
needs_bundle = pytest.mark.skipif(
    not (BUNDLE / "bin" / "cascade").exists(), reason="needs a built bundle: bundle/build.sh"
)


def project(name: str, tmp_path: Path, monkeypatch) -> Path:
    """A copy of ``examples/<name>`` to run in, with the bundle path made absolute: the
    example's relative path only holds inside the repo."""
    dest = tmp_path / name
    shutil.copytree(EXAMPLES / name, dest, ignore=shutil.ignore_patterns("_store"))
    deployment = dest / "deployment.yaml"
    doc = yaml.safe_load(deployment.read_text())
    for backend in doc.get("backends", []):
        if "bundle" in (backend.get("config") or {}):
            backend["config"]["bundle"] = str(BUNDLE)
    deployment.write_text(yaml.safe_dump(doc))
    monkeypatch.chdir(dest)
    return dest


def run(dest: Path, capsys, run_id: str) -> dict:
    """``cascade run`` in ``dest``; returns each dag output's value.

    ``cascade run`` reports where outputs are, relative to the run's own scope (the
    deployment's store scope, the run id, the entrypoint dag), so they are read from
    there."""
    capsys.readouterr()
    assert main(["run", "pipeline.yaml", "--run-id", run_id]) == 0, capsys.readouterr().err
    deployment = yaml.safe_load((dest / "deployment.yaml").read_text())
    pipeline = yaml.safe_load((dest / "pipeline.yaml").read_text())
    store_config = deployment["store"]["config"]
    run_scope = (*store_config["scope"], run_id, pipeline["entrypoint"])
    store = FileStore(FileConfig(root=str(dest / store_config["root"]), scope=run_scope))
    values = {}
    for line in capsys.readouterr().out.splitlines()[1:]:
        port, _, path = line.strip().partition(": ")
        *scope, key = path.split("/")
        values[port] = store.read_json(key, at=tuple(scope))
    return values


@needs_docker
@needs_bundle
def test_docker_raw_runs_stock_images_wrapped_by_the_bundle(tmp_path, monkeypatch, capsys):
    dest = project("docker-raw", tmp_path, monkeypatch)
    assert run(dest, capsys, "t1") == {"result": [1, 2, 3, 4, 5]}
    # the run records the image each ref actually ran, by id
    record = FileStore(FileConfig(root=str(dest / "_store"), scope=("docker-raw", "t1")))
    images = record.get_json("images")
    assert set(images) == {"gen", "add-one"}
    assert images["gen"]["id"].startswith("sha256:")


@needs_docker
def test_docker_sdk_builds_an_image_with_cascade_inside_and_runs_it_unwrapped(
    tmp_path, monkeypatch, capsys
):
    dest = project("docker-sdk", tmp_path, monkeypatch)
    assert run(dest, capsys, "t1") == {"result": [1, 2, 3, 4, 5]}


@needs_docker
@needs_bundle
def test_docker_built_wrapped_builds_a_tool_image_and_wraps_it(tmp_path, monkeypatch, capsys):
    dest = project("docker-built-wrapped", tmp_path, monkeypatch)
    assert run(dest, capsys, "t1") == {"result": [1, 2, 3, 4, 5]}


def test_limits_holds_the_bulky_step_to_two_at_a_time(tmp_path, monkeypatch, capsys):
    """Four 3 s `bulky-task` lanes under `limits: {bulky: 2}` take two rounds, ~6 s; the
    backend's `concurrency: 4` alone would let them finish in one, ~3 s."""
    dest = project("limits", tmp_path, monkeypatch)
    started = time.monotonic()
    values = run(dest, capsys, "t1")
    elapsed = time.monotonic() - started
    assert values == {"squares": [225.0, 272.25, 324.0, 380.25], "total": 1201.5}
    assert elapsed >= 6
