"""Real child processes for kill/cancel tests: they announce their pid in a file, then
wait, so a test can check afterwards whether anything is still running."""
from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path


def sleeper(pidfile: Path, *, ignore_term: bool = False, seconds: int = 30) -> list[str]:
    """A command that records its pid in ``pidfile`` and sleeps. With ``ignore_term`` it
    shrugs off SIGTERM, so only SIGKILL stops it. The pid is written *after* the signal
    disposition is set, so a test that waits for the file never races it."""
    code = (
        "import os, signal, sys, time\n"
        + ("signal.signal(signal.SIGTERM, signal.SIG_IGN)\n" if ignore_term else "")
        + "open(sys.argv[1], 'w').write(str(os.getpid()))\n"
        + f"time.sleep({seconds})\n"
    )
    return [sys.executable, "-c", code, str(pidfile)]


def fails_after(pidfile: Path, code: int = 3) -> list[str]:
    """A command that waits until ``pidfile`` exists (a sibling has started), then exits
    with ``code`` — so the failure always lands while the sibling is running."""
    script = (
        "import os, sys, time\n"
        "while not os.path.exists(sys.argv[1]): time.sleep(0.02)\n"
        f"sys.exit({code})\n"
    )
    return [sys.executable, "-c", script, str(pidfile)]


async def pid_from(pidfile: Path, timeout: float = 10.0) -> int:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pidfile.exists() and pidfile.read_text():
            return int(pidfile.read_text())
        await asyncio.sleep(0.02)
    raise TimeoutError(f"no pid in {pidfile}")


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


posix_only = sys.platform == "win32"
"""Signal semantics (SIGTERM vs SIGKILL, pid probing) are POSIX; skip there on Windows."""
