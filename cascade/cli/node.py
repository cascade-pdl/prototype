"""``cascade node inputs`` / ``cascade node outputs`` — the prebuilt shim.

These are what let an **unmodified tool** become a ref without anybody writing Python. A
wrapper image becomes a shell entrypoint:

    ENTRYPOINT ["sh", "-c", "cascade node inputs \\
      && python -m birdnet_analyzer.analyze /cascade/inputs/recordings -o /cascade/outputs/detections \\
      && cascade node outputs"]

``inputs`` stages every bound input under ``/cascade/inputs/<port>`` in the encoding the
port declares; ``outputs`` reads ``/cascade/outputs/<port>``, converts back to canonical
form, writes to the store, **and emits the completion marker in the same call** — splitting
those two creates a class of bug where the outputs exist and the marker disagrees.

A model is therefore *not* entirely unmodified: it must agree on those paths. That is the
whole of the coupling, and it is what ``mapping`` or a thin per-ref shim bridges when a
tool insists on its own layout.

Note these commands do only what the library does; there is no capability here a
hand-written entrypoint could not have. That is deliberate — the protocol is the contract,
and this is one implementation of it.
"""
from __future__ import annotations

import os
import argparse
from pathlib import Path

from cascade.cli.errors import CliError
from cascade.node.node import DONE_MARKER
from cascade.node import Node, NodeError, from_env


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
    for binding in instance.inputs.inputs:
        try:
            if binding.depth > 0 and args.collections == "dir":
                target = instance.dir(binding.port)
                count = len(list(target.iterdir()))
                print(f"staged {binding.port} -> {target}/ ({count} file(s))")
            else:
                target = instance.path(binding.port)
                print(f"staged {binding.port} -> {target}")
        except NodeError as e:
            raise CliError(str(e))
    return 0


def cmd_outputs(args: argparse.Namespace) -> int:
    """Collect every declared output, then write the completion marker."""
    instance = _node(args)
    outputs_dir = instance.root / "outputs"
    if not instance.outputs.ports:
        raise CliError(
            "no output ports declared; the runner sets CASCADE_OUTPUTS from the plan"
        )

    for decl in instance.outputs.outputs:
        produced = outputs_dir / decl.port
        try:
            if produced.is_dir():
                instance.write_dir(decl.port, produced)
                print(f"collected {decl.port} <- {produced}/")
            elif produced.is_file():
                instance.write_file(decl.port, produced)
                print(f"collected {decl.port} <- {produced}")
            else:
                raise CliError(
                    f"declared output {decl.port!r} not found at {produced} — a ref must "
                    f"write each output port to /cascade/outputs/<port>"
                )
        except NodeError as e:
            raise CliError(str(e))

    try:
        instance.done()
    except NodeError as e:
        raise CliError(str(e))
    print(f"wrote {DONE_MARKER}")
    return 0


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