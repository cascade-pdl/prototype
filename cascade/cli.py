"""``cascade`` — the command-line interface.

    cascade validate <pipeline.yaml>     parse, validate refs and dag connections
    cascade graph    <pipeline.yaml>     print the resolved execution waves
    cascade run      <pipeline.yaml> --input name=key ...  --store DIR
    cascade query    <run_id> --store DIR [--node NODE [--instance INST]]
"""

from __future__ import annotations

import argparse
import sys

from .engine import Engine
from .loader import load_pipeline
from .plan import build_plan
from .runner import SubprocessRunner, EchoRunner
from .store import FileStore
from .validate import ValidationReport, validate_dags, validate_refs


def _print_report(report: ValidationReport) -> bool:
    if not report.diagnostics:
        print(f"  {report.phase}: ok")
        return True
    for d in report.diagnostics:
        marker = "ERROR" if d.severity == "error" else "warn "
        print(f"  [{marker}] {d.location}: {d.message}")
    print(f"  {report.phase}: {'ok (with warnings)' if report.ok else 'FAILED'}")
    return report.ok


def cmd_validate(args) -> int:
    pipeline = load_pipeline(args.pipeline)
    print("validating refs...")
    refs_ok = _print_report(validate_refs(pipeline))
    print("validating dag connections...")
    dags_ok = _print_report(validate_dags(pipeline))
    if refs_ok and dags_ok:
        print("\nok: pipeline is valid")
        return 0
    print("\nvalidation failed")
    return 1


def cmd_graph(args) -> int:
    pipeline = load_pipeline(args.pipeline)
    plan = build_plan(pipeline)
    for i, wave in enumerate(plan.waves, 1):
        annotated = []
        for nid in wave:
            n = plan.nodes[nid]
            tag = f" (scatter:{n.scatter})" if n.scatter else ""
            annotated.append(f"{nid}{tag}")
        print(f"wave {i}: {', '.join(annotated)}")
    return 0


def cmd_run(args) -> int:
    pipeline = load_pipeline(args.pipeline)
    report = validate_dags(pipeline)
    if not report.ok:
        print("refusing to run: dag validation failed")
        _print_report(report)
        return 1

    plan = build_plan(pipeline)
    store = FileStore(args.store)

    inputs = {}
    for item in args.input or []:
        if "=" not in item:
            print(f"--input must be name=key, got '{item}'")
            return 1
        name, key = item.split("=", 1)
        inputs[name] = key

    # assemble extra docker arguments from --docker-arg (raw) and --docker-env
    extra_args = []
    for env_item in args.docker_env or []:
        if "=" not in env_item:
            print(f"--docker-env must be NAME=VALUE, got '{env_item}'")
            return 1
        extra_args += ["-e", env_item]
    for raw in args.docker_arg or []:
        extra_args += raw.split()

    if args.dry_run:
        engine = Engine(pipeline, store, EchoRunner())
        state = engine.run(plan, inputs, run_id=args.run_id)
        print(f"\nrun {state.run_id}: {state.status}")
        return 0 if state.status == "complete" else 1

    # deployment config: from --runner-config file (YAML) if given, else empty
    import yaml as _yaml
    from .runners_config import DeploymentConfig, RunnerKind
    from .runner import RunnerRegistry, check_deployment_satisfies
    deploy_raw = None
    if args.runner_config:
        deploy_raw = _yaml.safe_load(open(args.runner_config))
    deployment = DeploymentConfig.from_dict(deploy_raw)

    # fail fast: does this deployment satisfy every runner kind the pipeline needs?
    required = {r.runner.kind for r in pipeline.refs}
    problems = check_deployment_satisfies(required, deployment)
    if problems:
        print("refusing to run: deployment does not satisfy required runners:")
        for p in problems:
            print(f"  - {p}")
        return 1

    # the project name (from cascade.toml, if present) lets the ECS runner derive
    # the conventional taskdef family — matching what provisioning created
    project_name = None
    try:
        from .project import ProjectConfig
        import os
        if os.path.exists("cascade.toml"):
            project_name = ProjectConfig.load("cascade.toml").name
    except Exception:
        project_name = None

    registry = RunnerRegistry(deployment, store_root=args.store,
                              subprocess_extra_args=extra_args,
                              project_name=project_name)
    engine = Engine(pipeline, store, runners=registry,
                    max_concurrency=args.max_concurrency,
                    store_conf=deployment.store)
    state = engine.run(plan, inputs, run_id=args.run_id)
    print(f"\nrun {state.run_id}: {state.status}")
    print(f"run state: runs/{state.run_id}/_run_state.json (in {args.store})")
    return 0 if state.status == "complete" else 1


def cmd_query(args) -> int:
    store = FileStore(args.store)
    key = f"runs/{args.run_id}/_run_state.json"
    if not store.has(key):
        print(f"no run state found for '{args.run_id}' in {args.store}")
        return 1
    state = store.get_json(key)
    if args.node:
        node = state["nodes"].get(args.node)
        if not node:
            print(f"no node '{args.node}' in run '{args.run_id}'")
            return 1
        for inst in node["instances"]:
            if args.instance and inst["instance_key"] != args.instance:
                continue
            print(f"{args.node}[{inst['instance_key']}]: {inst['status']} "
                  f"-> {inst.get('output_key')}")
        return 0
    # whole-run summary
    print(f"run {state['run_id']}: {state['status']}")
    for nid, node in state["nodes"].items():
        n_inst = len(node["instances"])
        print(f"  {nid}: {node['status']} ({n_inst} instance(s))")
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="cascade", description="declarative container pipelines")
    sub = parser.add_subparsers(dest="command", required=True)

    p_val = sub.add_parser("validate", help="validate a pipeline")
    p_val.add_argument("pipeline")
    p_val.set_defaults(func=cmd_validate)

    p_graph = sub.add_parser("graph", help="print execution waves")
    p_graph.add_argument("pipeline")
    p_graph.set_defaults(func=cmd_graph)

    # node-side utilities (run inside the container by the entrypoint)
    from .node_cli import add_node_subcommands
    add_node_subcommands(sub)

    # authoring + provisioning command groups
    from .provisioning import add_authoring_subcommands, add_provisioning_subcommands
    add_authoring_subcommands(sub)
    add_provisioning_subcommands(sub)

    p_run = sub.add_parser("run", help="run a pipeline")
    p_run.add_argument("pipeline")
    p_run.add_argument("--store", default="./_cascade_store", help="store directory")
    p_run.add_argument("--store-mount", default=None,
                       help="host:container bind mount for the store (subprocess runner)")
    p_run.add_argument("--input", action="append", help="pipeline input as name=storekey")
    p_run.add_argument("--docker-arg", action="append",
                       help="extra raw arg(s) passed to 'docker run', repeatable. "
                            "e.g. --docker-arg '-v /home/me/.aws:/root/.aws:ro'")
    p_run.add_argument("--docker-env", action="append",
                       help="env var passed into the container as NAME=VALUE, repeatable. "
                            "e.g. --docker-env AWS_PROFILE=wilder-sensing-develop")
    p_run.add_argument("--run-id", default=None)
    p_run.add_argument("--runner-config", default=None,
                       help="deployment config YAML (ECS cluster, etc.) — per-environment, "
                            "kept out of the pipeline")
    p_run.add_argument("--max-concurrency", type=int, default=1,
                       help="max node instances to run at once (scatter fan-out). "
                            "Default 1 = sequential; raise for parallel runs (e.g. ECS).")
    p_run.add_argument("--dry-run", action="store_true", help="use the echo runner, launch nothing")
    p_run.set_defaults(func=cmd_run)

    p_q = sub.add_parser("query", help="query a run's state / results")
    p_q.add_argument("run_id")
    p_q.add_argument("--store", default="./_cascade_store")
    p_q.add_argument("--node", default=None)
    p_q.add_argument("--instance", default=None)
    p_q.set_defaults(func=cmd_query)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
