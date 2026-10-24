"""``place`` — where the plan meets the deployment, before anything runs.

Lives with the executor because the executor is the only holder of a ``Deployment``
Collects every problem at once, the way ``check`` does at compile time:
refs that fit no backend, and a deployment that cannot work as a whole (an ECS backend
with a file store). An unplaceable ref found mid-run would leave half a run behind.
"""
from __future__ import annotations

from cascade.deployment import Deployment
from cascade.plan.plan import Plan
from cascade.substrate.backends import uncapped_local
from cascade.substrate.placement import PlacementError, PlacementMap, place_refs


def place(plan: Plan, deployment: Deployment | None) -> PlacementMap:
    """Every placed ref's backend, or a ``PlacementError`` listing every problem.

    With no deployment (tests, ``cascade run --backend``) everything goes to the
    uncapped local host.
    """
    if deployment is None:
        return place_refs(plan, [uncapped_local()])
    errors = deployment.check()
    try:
        placements = place_refs(plan, deployment.backends)
    except PlacementError as e:
        errors += e.errors
    if errors:
        raise PlacementError(errors)
    return placements
