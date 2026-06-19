"""The `run` verb: execute a pipeline against a deployment."""

from __future__ import annotations

import os

from ..engine import Engine
from ..loader import load_pipeline
from ..plan import build_plan
from ..validate import validate_dags
from ..runner import RunnerRegistry, check_deployment_satisfies, EchoRunner
from .utils import load_deployment, load_project, store_from_deployment, parse_inputs, print_report


def cmd_run(args) -> int:
    pipeline = load_pipeline(args.pipeline)
    report = validate_dags(pipeline)
    if not report.ok:
        print("refusing to run: dag validation failed")
        print_report(report)
        return 1
    plan = build_plan(pipeline)
    inputs = parse_inputs(args.input)

    deployment, deploy_path = load_deployment(args)
    store = store_from_deployment(deployment, args)

    if args.dry_run:
        engine = Engine(pipeline, store, EchoRunner(), store_conf=deployment.store)
        state = engine.run(plan, inputs, run_id=args.run_id)
        print(f"\nrun {state.run_id}: {state.status}")
        return 0 if state.status == "complete" else 1

    # fail fast: does the deployment satisfy every runner kind the pipeline needs?
    required = {r.runner.kind for r in pipeline.refs}
    problems = check_deployment_satisfies(required, deployment)
    if problems:
        print("refusing to run: deployment does not satisfy required runners:")
        for p in problems:
            print(f"  - {p}")
        if deploy_path is None:
            print("  (no deployment config loaded — pass --runner-config, or put a "
                  "deployment.yaml in the working directory)")
        return 1

    # project name (from cascade.toml, if present) lets the ECS runner derive the
    # conventional taskdef family — matching what provisioning created
    project = load_project(args, required=False)
    project_name = project.name if project else None

    from ..store_config import StoreKind
    store_root = args.store if deployment.store.kind == StoreKind.file else None
    registry = RunnerRegistry(deployment, store_root=store_root, project_name=project_name)
    engine = Engine(pipeline, store, runners=registry,
                    max_concurrency=args.max_concurrency, store_conf=deployment.store)
    state = engine.run(plan, inputs, run_id=args.run_id)
    print(f"\nrun {state.run_id}: {state.status}")
    print(f"run state: runs/{state.run_id}/_run_state.json")
    return 0 if state.status == "complete" else 1


def add_subcommands(sub):
    p = sub.add_parser("run", help="run a pipeline")
    p.add_argument("pipeline")
    p.add_argument("--runner-config", default=None,
                   help="deployment config YAML (runners + store). If omitted, "
                        "./deployment.yaml is used when present.")
    p.add_argument("--input", action="append", help="pipeline input as name=storekey")
    p.add_argument("--run-id", default=None)
    p.add_argument("--project-file", default="cascade.toml")
    p.add_argument("--store", default="./_cascade_store",
                   help="file-store root (only when the deployment store is a "
                        "local file store; ignored for S3)")
    p.add_argument("--max-concurrency", type=int, default=1,
                   help="max node instances to run at once (scatter fan-out). "
                        "Default 1 = sequential; raise for parallel runs (e.g. ECS).")
    p.add_argument("--dry-run", action="store_true", help="use the echo runner, launch nothing")
    p.set_defaults(func=cmd_run)
