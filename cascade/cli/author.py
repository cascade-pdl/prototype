"""Author commands: pure transforms on files. They take a pipeline or a plan and
produce output, depending on no ambient project state — so they stay good CI and
scripting citizens (same inputs, same result, regardless of cwd)."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from yaml import safe_load

from cascade.model.pipeline import Pipeline
from cascade.plan.compile import compile_pipeline, check, CompileError
from cascade.plan.plan import Plan, PlanVersionError
from cascade.plan.integrity import check_plan_integrity
from cascade.plan.slice import find_orphans
from cascade.cli.errors import CliError


def _load_pipeline(path: str) -> Pipeline:
    try:
        text = Path(path).read_text()
    except FileNotFoundError:
        raise CliError(f"pipeline file not found: {path}")
    return Pipeline.decode(safe_load(text))


def _load_plan(path: str) -> Plan:
    try:
        raw = json.loads(Path(path).read_text())
    except FileNotFoundError:
        raise CliError(f"plan file not found: {path}")
    try:
        return Plan.decode(raw)
    except PlanVersionError as e:
        raise CliError(str(e))


def cmd_validate(args: argparse.Namespace) -> int:
    errors = check(_load_pipeline(args.pipeline))
    if errors:
        print(f"invalid ({len(errors)} error(s)):")
        for e in errors:
            print(f"  - {e}")
        return 1
    print("valid")
    return 0


def cmd_compile(args: argparse.Namespace) -> int:
    pipeline = _load_pipeline(args.pipeline)
    try:
        plan = compile_pipeline(pipeline)
    except CompileError as e:
        raise CliError(str(e))
    src = Path(args.pipeline)
    out = Path(args.output) if args.output else src.parent / (src.stem + ".plan.json")
    if str(out) == "-":
        print(json.dumps(plan.encode(), indent=2))
    else:
        out.write_text(json.dumps(plan.encode(), indent=2))
        print(f"wrote {out}")
    return 0


def cmd_show(args: argparse.Namespace) -> int:
    plan = _load_plan(args.plan)
    print(f"entrypoint: {plan.entrypoint}")
    print(f"version:    {plan.version}")
    print(f"dags:       {len(plan.node_graphs)}")
    for name, graph in plan.node_graphs.items():
        node_ids = [nid for nid, _ in graph.nodes()]
        print(f"  {name}  ({len(node_ids)} node(s)): {', '.join(node_ids)}")
        for dep in plan.dag_outputs.get(name, []):
            print(f"      -> {dep.node}.{dep.field} [{dep.mode}]")
    integrity = check_plan_integrity(plan)
    if integrity:
        print(f"integrity: {len(integrity)} problem(s)")
        for e in integrity:
            print(f"  - {e}")
    else:
        print("integrity: ok")
    return 0


def cmd_find_orphans(args: argparse.Namespace) -> int:
    orphans = find_orphans(_load_plan(args.plan))
    if not orphans:
        print("no orphans")
        return 0
    print(f"{len(orphans)} orphan(s):")
    for name in sorted(orphans):
        print(f"  - {name}")
    return 0


def add_parsers(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("validate", help="type-check a pipeline without compiling")
    p.add_argument("pipeline", help="pipeline YAML")
    p.set_defaults(func=cmd_validate)

    p = sub.add_parser("compile", help="compile a pipeline to a .plan.json")
    p.add_argument("pipeline", help="pipeline YAML")
    p.add_argument("-o", "--output", help="output path, or '-' for stdout "
                                          "(default: <pipeline>.plan.json)")
    p.set_defaults(func=cmd_compile)

    p = sub.add_parser("show", help="summarise a compiled plan")
    p.add_argument("plan", help="plan JSON")
    p.set_defaults(func=cmd_show)

    p = sub.add_parser("find-orphans", help="list nodes unreachable from the entrypoint")
    p.add_argument("plan", help="plan JSON")
    p.set_defaults(func=cmd_find_orphans)