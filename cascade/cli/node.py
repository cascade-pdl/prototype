"""``cascade node inputs`` / ``outputs`` / ``wrap`` — the prebuilt shim, as commands.

These let an **unmodified tool** become a ref without anybody writing Python. ``wrap`` is
the one a runner uses: it runs the other two around the tool's own command line::

    cascade node wrap -- python -m birdnet_analyzer.analyze \\
        /cascade/inputs/recordings -o /cascade/outputs/detections

The logic lives in the node SDK (``Node.stage_inputs``, ``Node.collect_outputs``,
``cascade.node.wrap``); this module only parses arguments, builds the ``Node`` from the
environment, and reports. ``inputs`` and ``outputs`` remain for entrypoints that want to
run the steps themselves.

A tool is therefore *not* entirely unmodified: it must agree on the paths
``/cascade/inputs/<port>`` and ``/cascade/outputs/<port>``. That is the whole of the
coupling, and it is what ``mapping`` or a thin per-ref shim bridges when a tool insists on
its own layout.
"""
from __future__ import annotations

import os
import argparse
from pathlib import Path

from cascade.cli.errors import CliError
from cascade.node.node import DONE_MARKER
from cascade.node import Node, NodeError, from_env, run_wrapped
from cascade.node.wrap import POST_HOOKS_FAILED, PRE_HOOKS_FAILED


def _node(args: argparse.Namespace) -> Node:
    try:
        instance = from_env(os.environ)
    except KeyError as e:
        raise CliError(f"missing environment: {e}; is this running under a cascade runner?")
    if args.root:
        instance.root = Path(args.root)
    return instance


def cmd_inputs(args: argparse.Namespace) -> int:
    """Stage every bound input as a local file or directory."""
    instance = _node(args)
    if not instance.inputs.inputs:
        print("no inputs to stage")
        return 0
    try:
        staged = instance.stage_inputs(args.collections)
    except NodeError as e:
        raise CliError(str(e))
    for port, path in staged:
        if path.is_dir():
            print(f"staged {port} -> {path}/ ({len(list(path.iterdir()))} file(s))")
        else:
            print(f"staged {port} -> {path}")
    return 0


def cmd_outputs(args: argparse.Namespace) -> int:
    """Collect every declared output, then write the completion marker."""
    try:
        collected = _node(args).collect_outputs()
    except NodeError as e:
        raise CliError(str(e))
    for port, path in collected:
        print(f"collected {port} <- {path}{'/' if path.is_dir() else ''}")
    print(f"wrote {DONE_MARKER}")
    return 0


def cmd_wrap(args: argparse.Namespace) -> int:
    """Stage inputs, run the command, and on success collect outputs: ``run_wrapped``."""
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        raise CliError("nothing to run: pass the command after `--`")
    return run_wrapped(_node(args), command, args.collections)


def add_parsers(sub: argparse._SubParsersAction) -> None:
    node_cmd = sub.add_parser(
        "node", help="container-side hooks: stage inputs, collect outputs"
    )
    node_sub = node_cmd.add_subparsers(dest="node_command", required=True)

    p = node_sub.add_parser("inputs", help="stage this node's inputs as local files")
    p.add_argument("--root", help="staging root (default: /cascade, or $CASCADE_ROOT)")
    p.add_argument(
        "--collections",
        choices=["dir", "file"],
        default="dir",
        help="stage a collection as a directory of elements (default) or one file",
    )
    p.set_defaults(func=cmd_inputs)

    p = node_sub.add_parser("outputs", help="collect this node's outputs and mark it done")
    p.add_argument("--root", help="staging root (default: /cascade, or $CASCADE_ROOT)")
    p.set_defaults(func=cmd_outputs)

    p = node_sub.add_parser(
        "wrap",
        help="stage inputs, run a command, then collect outputs and mark done",
        description="Run COMMAND between `inputs` and `outputs`. Its exit code is passed "
        "through; outputs are collected and the node marked done only if it exits 0. "
        "Wrapper failures exit %d (staging) or %d (collecting)."
        % (PRE_HOOKS_FAILED, POST_HOOKS_FAILED),
    )
    p.add_argument("--root", help="staging root (default: /cascade, or $CASCADE_ROOT)")
    p.add_argument(
        "--collections",
        choices=["dir", "file"],
        default="dir",
        help="stage a collection as a directory of elements (default) or one file",
    )
    p.add_argument("command", nargs=argparse.REMAINDER, help="-- COMMAND [ARG...]")
    p.set_defaults(func=cmd_wrap)