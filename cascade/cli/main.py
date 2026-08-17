"""Entry point: build the parser, dispatch, and translate expected failures.

``CliError`` means "the user can fix this" — it prints to stderr and exits 2 with
no traceback. Anything else propagates, because an unexpected exception is a bug
and its traceback is the useful output.
"""
from __future__ import annotations

import argparse
import sys

from cascade.cli import author, new, run, store
from cascade.cli.errors import CliError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cascade",
        description="Declarative, typed, container-native ML pipelines.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    new.add_parsers(sub)
    author.add_parsers(sub)
    run.add_parsers(sub)
    store.add_parsers(sub)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except CliError as e:
        print(f"cascade: error: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())