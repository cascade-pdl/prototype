"""Cascade CLI entry point.

The CLI is organised as a package: each command group is a module exposing
``add_subcommands(sub)``, and this module assembles them. Command *logic* lives
in importable library modules (engine, provisioning, ...) — the cli/ modules are
thin arg-parsing + dispatch, so the logic is testable without argparse.

Command map:
    validate, graph          top-level pipeline-analysis verbs
    run, query               top-level execution verbs (store from deployment)
    authoring  ...           create/inspect a project
    provisioning ...         generate infra artifacts (docker, ecs-task)
    store  fetch|stage|...   universal data-plane ops (store resolved by env/dep)
    node   before|after|...  in-container node-lifecycle ops (require node env)
"""

from __future__ import annotations

import argparse
import sys

from . import validate as _validate
from . import run as _run
from . import query as _query
from . import authoring as _authoring
from . import provisioning as _provisioning
from . import store as _store
from . import node as _node


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="cascade", description="declarative container pipelines")
    sub = parser.add_subparsers(dest="command", required=True)

    for mod in (_validate, _run, _query, _authoring, _provisioning, _store, _node):
        mod.add_subcommands(sub)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
