"""Top-level pipeline-analysis verbs: validate, graph. Pure pipeline inspection
(no store / deployment), so these are core commands, not authoring sub-modes."""

from __future__ import annotations

from ..loader import load_pipeline
from ..plan import build_plan
from ..validate import validate_dags, validate_refs
from .utils import print_report


def cmd_validate(args) -> int:
    pipeline = load_pipeline(args.pipeline)
    print("validating refs...")
    refs_ok = print_report(validate_refs(pipeline))
    print("validating dag connections...")
    dags_ok = print_report(validate_dags(pipeline))
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


def add_subcommands(sub):
    pv = sub.add_parser("validate", help="validate a pipeline")
    pv.add_argument("pipeline")
    pv.set_defaults(func=cmd_validate)

    pg = sub.add_parser("graph", help="print execution waves")
    pg.add_argument("pipeline")
    pg.set_defaults(func=cmd_graph)
