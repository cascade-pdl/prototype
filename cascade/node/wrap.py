"""Run an unmodified command as a node: stage its inputs, run it, collect its outputs.

This is what makes any image a ref. The image needs no cascade inside: the runner puts
cascade beside the tool (a self-contained bundle at ``/opt/cascade``) and makes it the
entrypoint, with the image's own entrypoint and command passed through::

    /opt/cascade/bin/cascade node wrap -- <the image's entrypoint and command...>

``run_wrapped`` then does three things, in order, each a function below:

1. ``_before``: stage every input under ``<root>/inputs/<port>`` and create
   ``<root>/outputs``. If this fails, the command never runs.
2. ``_run``: run the command with ``run_command`` (``cascade.node.process``), which
   forwards stop signals to it, and return its exit code unchanged.
3. ``_after``, only if the command exited 0: store every ``<root>/outputs/<port>`` and
   write the completion marker, last. A failed command leaves no marker and nothing
   collected, so a failed node never looks done.

The tool's half of the agreement is only "read ``inputs/<port>``, write
``outputs/<port>``"; ``Node.stage_inputs`` and ``Node.collect_outputs`` are cascade's half.
There is no capability here a hand-written entrypoint using ``Node`` could not have: this is
one implementation of the protocol, not the protocol.
"""
from __future__ import annotations

import signal
import sys
import traceback
from types import FrameType
from typing import Sequence

from cascade.node.node import Node, NodeError
from cascade.node.process import STOP_SIGNALS, run_command


PRE_HOOKS_FAILED = 120
"""Exit code when staging inputs failed; the command never ran."""
POST_HOOKS_FAILED = 121
"""Exit code when the command succeeded but collecting its outputs failed."""
CANNOT_EXECUTE = 126
COMMAND_NOT_FOUND = 127
"""As a shell reports them, so a missing tool reads the same with or without the wrapper.

None of these codes is reserved: a tool may exit with them too. The ``cascade node wrap:``
line on stderr is what says the wrapper failed, not the code alone."""


def run_wrapped(node: Node, command: Sequence[str], collections: str = "dir") -> int:
    """Run ``command`` as ``node``: stage inputs, run it, and on success collect outputs.

    Returns the exit code to exit with: the command's own (128 + n if a signal killed
    it), 0 once the outputs are stored and the node marked done, or one of the codes
    above if the wrapper itself failed. A stop signal (SIGTERM, SIGINT) is forwarded to
    the running command; one that arrives before or after it runs ends the run at once
    with 128 + n, writing no marker.

    Installs signal handlers for the duration and restores the previous ones on return,
    so it must be called from the main thread. ``collections`` is how collection inputs
    are staged (see ``Node.stage_inputs``).
    """
    if not command:
        raise ValueError("nothing to run")
    # outside the command's lifetime a stop ends the run; while it runs, run_command
    # replaces this handler with forwarding, and puts it back when the command exits
    previous = {s: signal.signal(s, _stop) for s in STOP_SIGNALS}
    try:
        return _phases(node, command, collections)
    except _Stopped as e:
        return 128 + e.signum
    finally:
        for s, handler in previous.items():
            signal.signal(s, handler)


def _phases(node: Node, command: Sequence[str], collections: str) -> int:
    try:
        _before(node, collections)
    except Exception:
        _complain("staging inputs")
        return PRE_HOOKS_FAILED

    code = _run(command)
    if code != 0:
        return code

    try:
        _after(node)
    except Exception:
        _complain("collecting outputs")
        return POST_HOOKS_FAILED
    return 0


def _before(node: Node, collections: str) -> None:
    for port, path in node.stage_inputs(collections):
        node.log(f"staged {port} -> {path}")
    # the tool's half of the agreement is only "write outputs/<port>", so the directory
    # has to exist before it runs
    node.outputs_dir.mkdir(parents=True, exist_ok=True)


def _run(command: Sequence[str]) -> int:
    """``run_command``, with a command that cannot start reported as a shell would."""
    try:
        return run_command(command)
    except FileNotFoundError as e:
        print(f"cascade node wrap: cannot run {command[0]!r}: {e}", file=sys.stderr)
        return COMMAND_NOT_FOUND
    except OSError as e:
        print(f"cascade node wrap: cannot run {command[0]!r}: {e}", file=sys.stderr)
        return CANNOT_EXECUTE


def _after(node: Node) -> None:
    for port, path in node.collect_outputs():
        node.log(f"collected {port} <- {path}")


def _complain(phase: str) -> None:
    """Every wrapper failure is reported, expected or not: the exit code alone is
    ambiguous (see above), so for anything but a ``NodeError`` the traceback is the
    useful output, even when the failure is a bug."""
    e = sys.exc_info()[1]
    if isinstance(e, NodeError):
        print(f"cascade node wrap: {phase} failed: {e}", file=sys.stderr)
    else:
        print(f"cascade node wrap: {phase} failed:", file=sys.stderr)
        traceback.print_exc()


class _Stopped(BaseException):
    """A stop signal arrived while no command was running: during the hooks. A
    ``BaseException``, so no ``except Exception`` inside a hook can swallow the stop."""

    def __init__(self, signum: int):
        self.signum = signum


def _stop(signum: int, _frame: FrameType | None) -> None:
    raise _Stopped(signum)
