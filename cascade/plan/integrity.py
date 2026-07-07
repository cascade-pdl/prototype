"""Plan-integrity check — the load-time, trusted-artifact twin of compile.check.

This is NOT re-validation. The authoring layer (compile.check) already proved the
pipeline well-formed and type-consistent against the full declared set; by the
time a Plan exists, that guarantee holds. This guards against a *corrupted*,
hand-edited, or version-mismatched .plan, the same role as the version field. It
is cheap set arithmetic over the maps the plan already carries — no graph build,
no type work.

Because run_config is ref-keyed and node_graphs is dag-keyed, their key sets *are*
the ref and dag namespaces, which is exactly the ``defined`` set the namespace
check needs — the thing bare node graphs lacked before run_config was carried.
"""
from __future__ import annotations

from cascade.plan.plan import Plan


def check_plan_integrity(plan: Plan) -> list[str]:
    """Return a list of integrity problems (empty = OK)."""
    errors: list[str] = []

    dags = set(plan.node_graphs)
    refs = set(plan.run_config)
    defined = dags | refs

    # refs and dags must be disjoint (flat-namespace uniqueness)
    overlap = dags & refs
    if overlap:
        errors.append(f"names are both ref and dag: {sorted(overlap)}")

    # every called runnable must be defined (a ref or a dag)
    for dag_name, g in plan.node_graphs.items():
        for _id, node in g.nodes():
            if node.runnable_name not in defined:
                errors.append(
                    f"dag {dag_name!r}: node {node.name!r} runs "
                    f"{node.runnable_name!r}, which is not a defined ref or dag"
                )

    # signatures must cover exactly the defined set (every runnable has one)
    sig = set(plan.signatures)
    if sig != defined:
        missing = sorted(defined - sig)
        extra = sorted(sig - defined)
        if missing:
            errors.append(f"signatures missing for: {missing}")
        if extra:
            errors.append(f"signatures for undefined runnables: {extra}")

    # the entrypoint must resolve
    if plan.entrypoint not in defined:
        errors.append(f"entrypoint {plan.entrypoint!r} is not a defined ref or dag")

    return errors
