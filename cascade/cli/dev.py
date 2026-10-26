"""``cascade dev`` — local stand-ins for development.

``cascade dev garage up`` starts a local S3 store (garage) with fixed values, so the
deployment pointing at it is a constant you commit; ``env`` prints the keys for the shell
that runs ``cascade run``, which serves them to containers.
"""
from __future__ import annotations

import argparse

import yaml

from cascade.cli.errors import CliError
from cascade.dev import garage


def cmd_up(args: argparse.Namespace) -> int:
    try:
        garage.up(garage.DEV)
    except garage.GarageError as e:
        raise CliError(str(e))
    block = yaml.safe_dump({"store": garage.DEV.store(["<project>"])}, sort_keys=False)
    print(f"garage is up at {garage.DEV.endpoint} (bucket {garage.BUCKET!r}, region "
          f"{garage.REGION!r}); data kept in volume {garage.DEV.volume!r}.\n")
    print("A deployment's store block for it:\n")
    print("  " + block.replace("\n", "\n  ").rstrip())
    print("\nIts keys, for the shell that runs `cascade run`:\n")
    print('  eval "$(cascade dev garage env)"')
    return 0


def cmd_down(args: argparse.Namespace) -> int:
    garage.down(garage.DEV, purge=args.purge)
    print("garage removed" + (", with its data and keys" if args.purge else
                              "; data and keys kept (--purge removes them)"))
    return 0


def cmd_env(args: argparse.Namespace) -> int:
    keys = garage.keys(garage.DEV)
    if keys is None:
        raise CliError("no garage keys yet: run `cascade dev garage up` first")
    print("\n".join(garage.env_lines(keys)))
    return 0


def add_parsers(sub) -> None:
    dev = sub.add_parser("dev", help="local stand-ins for development")
    dev_sub = dev.add_subparsers(dest="dev_command", required=True)
    g = dev_sub.add_parser("garage", help="a local S3 store (garage) in docker")
    g_sub = g.add_subparsers(dest="garage_command", required=True)

    p = g_sub.add_parser("up", help="start it and set it up (keeps what exists)")
    p.set_defaults(func=cmd_up)
    p = g_sub.add_parser("down", help="remove the container")
    p.add_argument("--purge", action="store_true", help="also remove its data and keys")
    p.set_defaults(func=cmd_down)
    p = g_sub.add_parser("env", help="print shell lines exporting its keys")
    p.set_defaults(func=cmd_env)
