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

    runner = EchoRunner() if args.dry_run else SubprocessRunner(store_mount=args.store_mount)
    engine = Engine(pipeline, store, runner)
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

    p_run = sub.add_parser("run", help="run a pipeline")
    p_run.add_argument("pipeline")
    p_run.add_argument("--store", default="./_cascade_store", help="store directory")
    p_run.add_argument("--store-mount", default=None,
                       help="host:container bind mount for the store (subprocess runner)")
    p_run.add_argument("--input", action="append", help="pipeline input as name=storekey")
    p_run.add_argument("--run-id", default=None)
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
