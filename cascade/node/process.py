"""Run a command from a node so that a stop reaches it.

A node's entrypoint is usually PID 1 in its container, and the kernel drops signals PID 1
has no handler for. So when an entrypoint shells out to a tool (``fb_predict -i ... -o
...``), ``docker stop`` and ``StopTask`` never reach the tool: they wait out their grace
period and then SIGKILL it, mid-write. ``run_command`` is ``subprocess.run`` with that
fixed: while the command runs, SIGTERM and SIGINT are forwarded to it.

    with session(os.environ) as n:
        n.stage_inputs()
        code = run_command(["fb_predict", "-i", "/cascade/inputs/images",
                            "-o", str(n.outputs_dir / "detections")])
        if code:
            sys.exit(code)
        n.collect_outputs()

``cascade node wrap`` (``cascade.node.wrap``) is built on it.
"""
from __future__ import annotations

import signal
import subprocess
from types import FrameType
from typing import Any, Sequence


STOP_SIGNALS = (signal.SIGTERM, signal.SIGINT)
"""What a container runtime or a terminal sends to stop a process."""


def run_command(command: Sequence[str], **popen_kwargs: Any) -> int:
    """Run ``command``, forwarding stop signals to it, and return its exit code.

    The code is reported as a shell does: 128 + n if signal n killed the command.
    ``popen_kwargs`` go to ``subprocess.Popen`` (``cwd``, ``env``, ``stdout``, ...). A
    command that cannot be started raises, as ``Popen`` does (``FileNotFoundError``,
    ``PermissionError``).

    Signal handling is changed only while the command is starting or running; the
    previous handlers are restored on return, so calls nest. A stop that arrives while
    the command is starting is held and forwarded once it has started: forwarding is
    impossible before then, and blocking the signals instead would not work, because the
    command would inherit the blocked mask and never hear them. If the command then fails
    to start, a held stop is delivered to the previous handlers instead, so it is not lost.

    Must be called from the main thread, where Python runs signal handlers.
    """
    if not command:
        raise ValueError("nothing to run")
    child: subprocess.Popen | None = None
    held: list[int] = []

    def forward(signum: int, _frame: FrameType | None) -> None:
        if child is None:
            held.append(signum)
        else:
            child.send_signal(signum)

    previous = {s: signal.signal(s, forward) for s in STOP_SIGNALS}
    try:
        child = subprocess.Popen(list(command), **popen_kwargs)
        for signum in held:
            child.send_signal(signum)
        code = child.wait()
    finally:
        for s, handler in previous.items():
            signal.signal(s, handler)
        if child is None:
            for signum in held:
                signal.raise_signal(signum)
    return 128 - code if code < 0 else code
