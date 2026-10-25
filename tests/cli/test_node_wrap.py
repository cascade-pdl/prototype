"""``cascade node wrap``: the hooks around an unmodified command.

Run as a real process with the real env contract, because what is under test is process
behaviour: exit codes, signals, and what is in the store afterwards.
"""
import json
import os
import signal
import subprocess
import sys
import time

import pytest

from cascade.node.wrap import COMMAND_NOT_FOUND, POST_HOOKS_FAILED, PRE_HOOKS_FAILED
from cascade.node.node import DONE_MARKER
from cascade.protocol.binding import InputBinding, InputBindings, OutputDecl, OutputDecls
from cascade.protocol.run_spec import RunSpec, to_env
from cascade.store.file_store import FileConfig, FileStore
from cascade.types import TypeExpr

posix_only = sys.platform == "win32"

# the "tool": upper-cases its input into its output, as a file-based tool would
UPPER = (
    "import pathlib; c = pathlib.Path({root!r}); "
    "(c / 'outputs' / 'upper').write_text((c / 'inputs' / 'text').read_text().upper())"
)


@pytest.fixture
def stores(tmp_path):
    base = FileConfig(root=str(tmp_path / "store"), scope=("r1", "main"))
    reader, writer = FileStore(base), FileStore(base.subscope(("upper",)))
    reader.put_json("text", "hello", at=("src",))
    return reader, writer


@pytest.fixture
def root(tmp_path):
    return tmp_path / "cascade"


@pytest.fixture
def env(stores, root):
    reader, writer = stores
    spec = RunSpec(
        name="upper",
        run_id="r1",
        node_id="upper",
        instance_id="r1/main/upper",
        store_in=reader,
        store_out=writer,
        inputs=InputBindings(
            (InputBinding("text", ("src",), "text", TypeExpr.parse("string")),)
        ),
        outputs=OutputDecls((OutputDecl("upper", TypeExpr.parse("string")),)),
    )
    return {**os.environ, **to_env(spec), "CASCADE_ROOT": str(root)}


def wrap(env, *command, **kw):
    argv = [sys.executable, "-m", "cascade.cli", "node", "wrap", "--", *command]
    return subprocess.run(argv, env=env, capture_output=True, text=True, timeout=60, **kw)


def python(code: str) -> list[str]:
    return [sys.executable, "-c", code]


def test_inputs_are_staged_the_command_runs_and_outputs_are_collected(stores, root, env):
    """The tool writes ``outputs/upper`` without creating ``outputs/`` first: the tool's
    half of the contract is only "write /cascade/outputs/<port>", so making the directory
    is the wrapper's job, and this test fails if it does not."""
    r = wrap(env, *python(UPPER.format(root=str(root))))
    assert r.returncode == 0, r.stderr
    _, writer = stores
    assert json.loads(writer.get("upper")) == "HELLO"
    assert writer.has(DONE_MARKER)


def test_a_failing_command_passes_its_code_through_and_marks_nothing(stores, root, env):
    code = UPPER.format(root=str(root)) + "; raise SystemExit(7)"
    r = wrap(env, *python(code))
    assert r.returncode == 7
    _, writer = stores
    # the output file was written, but a failed node's outputs are not collected
    assert not writer.has("upper")
    assert not writer.has(DONE_MARKER)


def test_a_missing_command_reads_as_a_shell_would_report_it(stores, env):
    r = wrap(env, "no-such-tool-anywhere")
    assert r.returncode == COMMAND_NOT_FOUND
    assert "cascade node wrap: cannot run 'no-such-tool-anywhere'" in r.stderr
    assert not stores[1].has(DONE_MARKER)


def test_a_staging_failure_never_runs_the_command(stores, tmp_path, env):
    (tmp_path / "store" / "r1" / "main" / "src" / "text").unlink()  # the input is gone
    ran = tmp_path / "ran"
    r = wrap(env, *python(f"open({str(ran)!r}, 'w')"))
    assert r.returncode == PRE_HOOKS_FAILED, r.stderr
    assert "cascade node wrap: staging inputs failed" in r.stderr
    assert not ran.exists()
    assert not stores[1].has(DONE_MARKER)


def test_a_command_that_writes_no_declared_output_fails_collection(stores, env):
    r = wrap(env, *python("pass"))
    assert r.returncode == POST_HOOKS_FAILED
    assert "cascade node wrap: collecting outputs failed" in r.stderr
    assert "/outputs/upper" in r.stderr
    assert not stores[1].has(DONE_MARKER)


def test_nothing_to_run_is_a_usage_error(env):
    r = subprocess.run(
        [sys.executable, "-m", "cascade.cli", "node", "wrap"],
        env=env, capture_output=True, text=True, timeout=60,
    )
    assert r.returncode == 2
    assert "nothing to run" in r.stderr


@pytest.mark.skipif(posix_only, reason="POSIX signals")
@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT])
def test_a_stop_signal_reaches_the_command(stores, tmp_path, env, signum):
    """``wrap`` is PID 1 in a container: unless it forwards, the tool never hears a stop
    and the runtime waits out its grace period before a SIGKILL."""
    ready, heard = tmp_path / "ready", tmp_path / "heard"
    tool = (
        "import signal, sys, time\n"
        f"def stop(n, f):\n    open({str(heard)!r}, 'w').write(str(n)); sys.exit(5)\n"
        "signal.signal(signal.SIGTERM, stop); signal.signal(signal.SIGINT, stop)\n"
        f"open({str(ready)!r}, 'w')\n"
        "time.sleep(60)\n"
    )
    proc = subprocess.Popen(
        [sys.executable, "-m", "cascade.cli", "node", "wrap", "--", *python(tool)],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
    )
    try:
        deadline = time.monotonic() + 30
        while not ready.exists():
            assert proc.poll() is None, proc.stderr.read()
            assert time.monotonic() < deadline, "the command never started"
            time.sleep(0.05)
        proc.send_signal(signum)
        assert proc.wait(timeout=10) == 5  # the tool's own exit code, passed through
    finally:
        proc.kill()
        proc.wait()
    assert heard.read_text() == str(int(signum))
    assert not stores[1].has(DONE_MARKER)


@pytest.mark.skipif(posix_only, reason="POSIX signals")
def test_a_command_killed_by_a_signal_exits_as_a_shell_reports_it(env):
    r = wrap(env, *python("import os, signal; os.kill(os.getpid(), signal.SIGKILL)"))
    assert r.returncode == 128 + signal.SIGKILL
