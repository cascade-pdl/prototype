"""``run_command``: a stop reaches the command, and signal handling is left as found.

Signals are sent to this test process itself, as a container runtime would send them to
the entrypoint. Every test runs with a recording handler installed first, so a broken
forward shows up as a failed assertion rather than a killed test run.
"""
import os
import signal
import subprocess
import sys
import threading
import time

import pytest

from cascade.node import process
from cascade.node.process import STOP_SIGNALS, run_command

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="POSIX signals")


def python(code: str) -> list[str]:
    return [sys.executable, "-c", code]


@pytest.fixture
def received():
    """Stops that reached *this* process's own handler instead of the command."""
    got: list[int] = []
    previous = {s: signal.signal(s, lambda n, f: got.append(n)) for s in STOP_SIGNALS}
    yield got
    for s, handler in previous.items():
        signal.signal(s, handler)


def handlers():
    return {s: signal.getsignal(s) for s in STOP_SIGNALS}


def test_the_exit_code_is_passed_through(received):
    assert run_command(python("raise SystemExit(4)")) == 4


def test_a_command_killed_by_a_signal_reports_as_a_shell_does(received):
    assert run_command(python("import os, signal; os.kill(os.getpid(), signal.SIGKILL)")) \
        == 128 + signal.SIGKILL


def test_popen_arguments_pass_through(received, tmp_path):
    assert run_command(python("open('made-here', 'w')"), cwd=tmp_path) == 0
    assert (tmp_path / "made-here").exists()


def test_a_stop_is_forwarded_to_the_running_command(received, tmp_path):
    ready, heard = tmp_path / "ready", tmp_path / "heard"
    tool = (
        "import signal, sys, time\n"
        f"def stop(n, f):\n    open({str(heard)!r}, 'w').write(str(n)); sys.exit(5)\n"
        "signal.signal(signal.SIGTERM, stop)\n"
        f"open({str(ready)!r}, 'w')\n"
        "time.sleep(20)\n"
    )

    def stop_when_ready():
        deadline = time.monotonic() + 20
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        os.kill(os.getpid(), signal.SIGTERM)

    threading.Thread(target=stop_when_ready, daemon=True).start()
    assert run_command(python(tool)) == 5  # the command's own handler ran
    assert heard.read_text() == str(int(signal.SIGTERM))
    assert received == []


def test_a_stop_while_starting_is_forwarded_once_started(received, monkeypatch):
    real = subprocess.Popen

    def popen(argv, **kw):
        os.kill(os.getpid(), signal.SIGTERM)  # lands before there is a child to forward to
        return real(argv, **kw)

    monkeypatch.setattr(process.subprocess, "Popen", popen)
    assert run_command(python("import time; time.sleep(20)")) == 128 + signal.SIGTERM
    assert received == []


def test_a_stop_held_for_a_command_that_never_starts_is_not_lost(received, monkeypatch):
    def popen(argv, **kw):
        os.kill(os.getpid(), signal.SIGTERM)
        raise FileNotFoundError(2, "No such file or directory", argv[0])

    monkeypatch.setattr(process.subprocess, "Popen", popen)
    with pytest.raises(FileNotFoundError):
        run_command(["no-such-tool"])
    assert received == [signal.SIGTERM]  # delivered to the handler that was there before


def test_a_missing_command_raises_and_restores_the_handlers(received):
    before = handlers()
    with pytest.raises(FileNotFoundError):
        run_command(["no-such-tool-anywhere"])
    assert handlers() == before


def test_the_handlers_are_restored_after_a_run(received):
    before = handlers()
    run_command(python("pass"))
    assert handlers() == before


def test_nothing_to_run_is_refused():
    with pytest.raises(ValueError, match="nothing to run"):
        run_command([])
