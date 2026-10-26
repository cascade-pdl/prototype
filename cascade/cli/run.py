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
from cascade.executor import Executor, ExecutorError, place
from cascade.runners.images import ImageBuildError, prepare_images
from cascade.substrate.backends import uncapped_local
from cascade.substrate.placement import PlacementError
from cascade.cli.errors import CliError
from cascade.cli._resolve import add_store_flags, build_store, resolve_deployment


def _load(path: str):
    """Accept either a pipeline to compile or an already-compiled plan."""
    text = Path(path).read_text()
    if path.endswith(".json"):
        try:
            return Plan.decode(json.loads(text))
        except PlanVersionError as e:
            raise CliError(str(e))
    try:
        return compile_pipeline(Pipeline.decode(safe_load(text)), base=Path(path).parent)
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
    deployment = resolve_deployment(args)
    # a ref's relative command resolves against the document that declared it, so the
    # same pipeline runs the same way from any working directory
    cwd = str(Path(args.pipeline).resolve().parent)
    try:
        executor = Executor(plan, deployment=deployment, store=store, cwd=cwd)
        result = asyncio.run(executor.run(inputs=inputs, run_id=args.run_id))
    except ExecutorError as e:
        raise CliError(str(e))

    print(f"run {result.run_id} complete")
    for port, (scope, key) in result.outputs.items():
        print(f"  {port}: {'/'.join((*scope, key))}")
    return 0


def cmd_build(args: argparse.Namespace) -> int:
    """Build the images the pipeline's ``build:`` refs need, without running anything.

    ``cascade run`` does the same before it starts; this is for building ahead (CI, or
    to see a Dockerfile fail on its own). Images that already exist are not rebuilt:
    their names are content hashes, so an existing name is an up-to-date image.
    """
    try:
        plan = _load(args.pipeline)
    except FileNotFoundError:
        raise CliError(f"not found: {args.pipeline}")
    deployment = resolve_deployment(args)
    try:
        placements = place(plan, deployment)
    except PlacementError as e:
        raise CliError("cannot place this pipeline:\n" + "\n".join(f"  - {m}" for m in e.errors))
    backends = deployment.backends if deployment else [uncapped_local()]
    cwd = str(Path(args.pipeline).resolve().parent)
    try:
        built = asyncio.run(prepare_images(plan, placements, backends, cwd))
    except ImageBuildError as e:
        raise CliError(str(e))
    for image in built:
        print(f"built {image}")
    if not built:
        print("all images up to date")
    return 0


def add_parsers(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("run", help="execute a pipeline or a compiled plan")
    p.add_argument("pipeline", help="pipeline YAML, or a .json plan")
    p.add_argument("--input", action="append", metavar="NAME=VALUE",
                   help="a run input; VALUE is parsed as JSON, else taken as a string")
    p.add_argument("--run-id", help="reuse a specific run id (default: minted)")
    add_store_flags(p)
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("build", help="build the images of the pipeline's `build:` refs")
    p.add_argument("pipeline", help="pipeline YAML, or a .json plan")
    add_store_flags(p)
    p.set_defaults(func=cmd_build)