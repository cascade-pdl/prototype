"""The built bundle (``bundle/build.sh``) running ``cascade node wrap`` inside images that
have no cascade, no Python, another libc, or nothing at all.

Opt-in: needs Docker and a built bundle, so it runs only when ``CASCADE_BUNDLE`` points at
the unpacked tree (``dist/bundle/opt-cascade``). Images are pulled if missing.

Each case mirrors how a runner will use the bundle: mounted read-only at ``/opt/cascade``,
the bundle's ``cascade`` as the entrypoint, a non-root user (the local docker runner maps
the host user), a writable ``/cascade``, and the host file store mounted inside it.
"""
import json
import os
import shutil
import subprocess

import pytest

from cascade.node.node import DONE_MARKER
from cascade.protocol.binding import InputBinding, InputBindings, OutputDecl, OutputDecls
from cascade.protocol.run_spec import RunSpec, to_env
from cascade.store.file_store import FileConfig, FileStore
from cascade.types import TypeExpr

BUNDLE = os.environ.get("CASCADE_BUNDLE")

pytestmark = pytest.mark.skipif(
    not BUNDLE or shutil.which("docker") is None,
    reason="set CASCADE_BUNDLE to a built bundle (bundle/build.sh) and have docker",
)

SCOPE = ("r1", "main")
UPPER_SH = ["sh", "-c", "tr a-z A-Z < /cascade/inputs/text > /cascade/outputs/upper"]
UPPER_PY = "import pathlib as p; c = p.Path('/cascade'); " \
    "(c/'outputs/upper').write_text((c/'inputs/text').read_text().upper())"

CASES = {
    # glibc, a shell, no python
    "debian": ("debian:trixie-slim", [], UPPER_SH),
    # musl
    "alpine": ("alpine:3.22", [], UPPER_SH),
    # its own python, with settings that would break ours if they reached it
    "python": ("python:3.13-slim", ["-e", "PYTHONHOME=/usr/local", "-e", "PYTHONPATH=/nope"],
               ["python3", "-c", UPPER_PY]),
    # FROM scratch: no shell, no libc. Its only binary is jq, which cannot write a file,
    # so the bundle's own interpreter plays the tool
    "scratch": ("ghcr.io/jqlang/jq:1.8.1", [],
                ["/opt/cascade/bin/python3", "-I", "-c", UPPER_PY]),
}


@pytest.mark.parametrize("case", CASES)
def test_the_bundle_wraps_a_command_in_an_unmodified_image(tmp_path, case):
    image, extra, command = CASES[case]
    store_root = tmp_path / "store"
    store_root.mkdir()
    FileStore(FileConfig(root=str(store_root), scope=SCOPE)).put_json(
        "text", "hello", at=("src",))

    inside = FileConfig(root="/cascade/store", scope=SCOPE)
    spec = RunSpec(
        name="upper", run_id="r1", node_id="upper", instance_id="r1/main/upper",
        store_in=FileStore(inside), store_out=FileStore(inside.subscope(("upper",))),
        inputs=InputBindings((InputBinding("text", ("src",), "text", TypeExpr.parse("string")),)),
        outputs=OutputDecls((OutputDecl("upper", TypeExpr.parse("string")),)),
    )
    argv = [
        "docker", "run", "--rm", "--pull", "missing", "--network", "none",
        "--user", f"{os.getuid()}:{os.getgid()}",
        "--tmpfs", "/cascade:mode=1777",
        "-v", f"{store_root}:/cascade/store",
        "-v", f"{os.path.abspath(BUNDLE)}:/opt/cascade:ro",
        "--entrypoint", "/opt/cascade/bin/cascade",
        *extra,
    ]
    for k, v in to_env(spec).items():
        argv += ["-e", f"{k}={v}"]
    argv += [image, "node", "wrap", "--", *command]

    r = subprocess.run(argv, capture_output=True, text=True, timeout=300)
    assert r.returncode == 0, r.stdout + r.stderr
    out = FileStore(FileConfig(root=str(store_root), scope=SCOPE + ("upper",)))
    assert json.loads(out.get("upper")) == "HELLO"
    assert out.has(DONE_MARKER)
