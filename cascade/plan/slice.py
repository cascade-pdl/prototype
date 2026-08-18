"""Slicing a Plan to a reachable set, and the orphan query that is its complement.

One reachability op (``reachable_from`` over the call graph derived from the node
graphs) underlies both:
  * ``slice_plan(plan, root)`` — project every map onto what ``root`` reaches. Used to
    scope a subdag's plan for a dag-container, or to trim the top plan by slicing from
    its own entrypoint.
  * ``find_orphans(plan)``     — the *complement*: declared minus reachable. Same
    computation, opposite disposition (report the leftover instead of dropping it).

The call graph is never stored; it is rebuilt here from ``plan.node_graphs`` each
time, because it is a projection of them (an undefined callee is invisible by
construction, which is fine for a trusted plan).
"""
from __future__ import annotations

from cascade.plan.plan import Plan
from cascade.plan.build import call_graph_of, reachable_from


def slice_plan(plan: Plan, root: str) -> Plan:
    """Return a smaller Plan containing only what is reachable from ``root``.

    Scopes node_graphs, signatures, and run_config together by one reachable set;
    ``type_env`` is kept whole (structures are cheap; finer type slicing is a later
    optimisation). The returned plan's entrypoint is ``root``."""
    call = call_graph_of(plan.node_graphs)
    keep = reachable_from(call, root)
    return Plan(
        entrypoint=root,
        node_graphs={n: g for n, g in plan.node_graphs.items() if n in keep},
        signatures={n: s for n, s in plan.signatures.items() if n in keep},
        run_config={n: c for n, c in plan.run_config.items() if n in keep},
        dag_outputs={n: o for n, o in plan.dag_outputs.items() if n in keep},
        type_env=plan.type_env,
        version=plan.version,
    )



def find_orphans(plan: Plan, include_imported: bool = True) -> set[str]:
    """Runnables declared in the plan but unreachable from the entrypoint.

    The complement of the trim. ``include_imported=False`` filters out names that
    look imported (qualified with a ``.``) — imported-but-unused is the expected
    case once imports exist, so a local-only view is usually what an author wants.
    (Imports are not implemented yet; the filter is the forward-looking string
    test and is harmless until then.)"""
    call = call_graph_of(plan.node_graphs)
    reachable = reachable_from(call, plan.entrypoint)
    declared = set(plan.node_graphs) | set(plan.run_config)
    orphans = declared - reachable
    if not include_imported:
        orphans = {n for n in orphans if "." not in n}
    return orphans