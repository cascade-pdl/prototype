"""``cascade placement`` — where each ref of a pipeline would run, without running it.

First fit is deterministic, but it cannot be read off a ref alone: it needs the pipeline
and the deployment together. This prints the answer the executor would reach::

    [{"ref": "flat-bug", "backend": "ecs-gpu"}, {"ref": "tidy", "backend": "local"}]

``--explain`` adds, per ref, the backends passed over and why. A ref that fits nowhere is
listed with ``"backend": null`` and the exit status is non-zero; deployment-wide problems
(an ECS backend with a file store) are reported the same way the executor reports them.
"""
from __future__ import annotations

import argparse
import json
import sys

from cascade.cli.errors import CliError
from cascade.cli.run import _load
from cascade.cli._resolve import _load_deployment
from cascade.deployment import Deployment, DeploymentError
from cascade.substrate.placement import decide


def _deployment(args: argparse.Namespace) -> Deployment:
    try:
        if args.deployment:
            return Deployment.load(args.deployment)
        return _load_deployment(args)
    except FileNotFoundError:
        raise CliError(f"deployment file not found: {args.deployment}")
    except DeploymentError as e:
        raise CliError(f"invalid deployment: {e}")


def cmd_placement(args: argparse.Namespace) -> int:
    try:
        plan = _load(args.pipeline)
    except FileNotFoundError:
        raise CliError(f"not found: {args.pipeline}")
    deployment = _deployment(args)

    decisions = decide(plan, deployment.backends)
    if args.ref is not None:
        decisions = [d for d in decisions if d.ref == args.ref]
        if not decisions:
            raise CliError(f"no placed ref named {args.ref!r} (echo refs are never placed)")

    print(json.dumps([d.encode(explain=args.explain) for d in decisions], indent=2))
    problems = deployment.check()
    for message in problems:
        print(f"error: {message}", file=sys.stderr)
    return 1 if problems or any(d.backend is None for d in decisions) else 0


def add_parsers(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("placement", help="show which backend each ref would run on")
    p.add_argument("pipeline", help="pipeline YAML, or a .json plan")
    p.add_argument("ref", nargs="?", help="show only this ref")
    p.add_argument("--explain", action="store_true",
                   help="also list the backends passed over and why")
    p.add_argument("--project", metavar="PATH",
                   help="start directory for cascade.toml discovery (default: cwd)")
    p.add_argument("--deployment", metavar="PATH",
                   help="deployment file to place against (default: the project's pointer)")
    p.set_defaults(func=cmd_placement)
