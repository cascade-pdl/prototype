"""``place`` — where the plan meets the deployment, before anything runs.

Lives with the executor because the executor is the only holder of a ``Deployment``
Collects every problem at once, the way ``check`` does at compile time:
refs that fit no backend, a deployment that cannot work as a whole (an ECS backend
with a file store), and a wrapped docker ref on a local backend that has no bundle to
wrap it with. An unplaceable ref found mid-run would leave half a run behind.
"""
from __future__ import annotations

from typing import Sequence

from cascade.deployment import Deployment
from cascade.model.ref_data import RefDocker
from cascade.plan.plan import Plan
from cascade.substrate.backends import Backend, LocalConfig, uncapped_local
from cascade.substrate.placement import PlacementError, PlacementMap, place_refs


def place(plan: Plan, deployment: Deployment | None) -> PlacementMap:
    """Every placed ref's backend, or a ``PlacementError`` listing every problem.

    With no deployment (tests, ``cascade run --backend``) everything goes to the
    uncapped local host.
    """
    backends = [uncapped_local()] if deployment is None else deployment.backends
    errors = [] if deployment is None else deployment.check()
    try:
        placements = place_refs(plan, backends)
    except PlacementError as e:
        errors += e.errors
    else:
        errors += _missing_bundles(plan, placements, backends)
    if errors:
        raise PlacementError(errors)
    return placements


def _missing_bundles(
    plan: Plan, placements: PlacementMap, backends: Sequence[Backend]
) -> list[str]:
    """Wrapped docker refs placed on a local backend that names no bundle.

    Whether the path exists is checked when the runner is built: it is relative to the
    pipeline's directory, which placement does not know.
    """
    by_name = {b.name: b for b in backends}
    wanting: dict[str, list[str]] = {}
    for p in placements:
        ref = plan.run_config[p.ref].config
        backend = by_name[p.backend]
        if (
            isinstance(ref, RefDocker)
            and ref.wrap
            and isinstance(backend.config, LocalConfig)
            and backend.config.bundle is None
        ):
            wanting.setdefault(backend.name, []).append(p.ref)
    return [
        f"backend {name!r} runs wrapped docker ref(s) {sorted(refs)} but has no `bundle` "
        "in its config: build one with bundle/build.sh and set `bundle:` to "
        "dist/bundle/opt-cascade, or set `wrap: false` on refs whose image has cascade "
        "inside"
        for name, refs in wanting.items()
    ]
