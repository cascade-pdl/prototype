"""``cascade run`` — execute a pipeline against the project's deployment.

The store commands' resolution rules apply unchanged: flags override the deployment,
and being outside a project with no ``--backend`` is a hard error rather than a silent
default.
"""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from yaml import safe_load

from cascade.model.pipeline import Pipeline
from cascade.plan.compile import CompileError, compile_pipeline
from cascade.plan.plan import Plan, PlanVersionError
from cascade.engine.executor import Executor, ExecutorError
from cascade.runners.registry import RunnerEnv
from cascade.cli.errors import CliError
from cascade.cli._resolve import add_store_flags, build_store


def _load(path: str):
    """Accept either a pipeline to compile or an already-compiled plan."""
    text = Path(path).read_text()
    if path.endswith(".json"):
        try:
            return Plan.decode(json.loads(text))
        except PlanVersionError as e:
            raise CliError(str(e))
    try:
        return compile_pipeline(Pipeline.decode(safe_load(text)))
    except CompileError as e:
        raise CliError(str(e))


def cmd_run(args: argparse.Namespace) -> int:
    try:
        plan = _load(args.pipeline)
    except FileNotFoundError:
        raise CliError(f"not found: {args.pipeline}")

    inputs = {}
    for item in args.input or []:
        if "=" not in item:
            raise CliError(f"--input expects name=json-value, got {item!r}")
        name, _, raw = item.partition("=")
        try:
            inputs[name] = json.loads(raw)
        except json.JSONDecodeError:
            inputs[name] = raw  # a bare string is the common case

    store = build_store(args)
    # a ref's relative command resolves against the document that declared it, so the
    # same pipeline runs the same way from any working directory
    env = RunnerEnv(cwd=str(Path(args.pipeline).resolve().parent))
    try:
        result = asyncio.run(
            Executor(plan, store=store, env=env).run(inputs=inputs, run_id=args.run_id)
        )
    except ExecutorError as e:
        raise CliError(str(e))

    print(f"run {result.run_id} complete")
    for port, (scope, key) in result.outputs.items():
        print(f"  {port}: {'/'.join((*scope, key))}")
    return 0


def add_parsers(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("run", help="execute a pipeline or a compiled plan")
    p.add_argument("pipeline", help="pipeline YAML, or a .json plan")
    p.add_argument("--input", action="append", metavar="NAME=VALUE",
                   help="a run input; VALUE is parsed as JSON, else taken as a string")
    p.add_argument("--run-id", help="reuse a specific run id (default: minted)")
    add_store_flags(p)
    p.set_defaults(func=cmd_run)