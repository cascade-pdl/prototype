"""Turning configuration into runners — the one place that knows how.

Two stages, because two different things vary at two different times:

- **run-wide settings** are fixed for a whole run: where each ref was placed, the backends
  it was placed on, where a relative command resolves, and each docker ref's image as
  resolved for the run. Bound once, by ``configure_ref_builder``.
- **ref data** varies per node: the ref's name, its own config and its resolved ports.
  Passed at call time.

The result is a closure carrying those values and nothing else — no plan, no pipeline, and
no ``Deployment``: a runner has no business with a store binding, and ``cwd`` is not in a
deployment file at all.

**Why the name is passed.** The runner kind used to decide the runner: every docker ref ran
the same way. Now the kind says *what* a ref is and placement says *where*
it runs, and two docker refs in one pipeline can land on different backends. That decision
needs the plan and the deployment together, so the executor makes it, once, and binds the
resulting ``PlacementMap`` here. The map is keyed by ref name, so the caller says which ref
it wants; ``DagRunner`` already has the name in hand, and learns nothing about the
deployment by passing it. Nothing is re-matched here.

``DagRunner`` used to carry a ``RunnerEnv`` and an overrides map purely to hand them to
``build_runner``: it stored both, threaded both through its recursion, and read neither.
Coordination carrying configuration through itself is what made the packages tangle.

**Refs only.** ``RunnerKind`` has no ``dag`` member, and a ``DagRunner`` is built from a
plan rather than from a ref's config — so the two are not alternatives, and a builder able
to return either would be conflating substrate with coordination. ``DagRunner`` keeps its
own recursion and asks this only for refs.
"""
from __future__ import annotations

from typing import Callable, Mapping, Sequence

from cascade.model.runner_kinds import RunnerKind
from cascade.plan.run_config import RunConfig
from cascade.plan.signature import Signature
from cascade.protocol.runner import Runner
from cascade.runners.docker import ImageConfig
from cascade.runners.registry import build_runner
from cascade.substrate.backends import Backend
from cascade.substrate.placement import PlacementError, PlacementMap


RefRunnerBuilder = Callable[[str, RunConfig, "Signature | None"], Runner]
"""Ref name and data in, runner out. The whole seam between coordination and substrate."""


def build_ref(
    name: str,
    config: RunConfig,
    signature: Signature | None = None,
    *,
    placements: PlacementMap | None = None,
    backends: Sequence[Backend] = (),
    cwd: str | None = None,
    images: Mapping[str, ImageConfig] | None = None,
) -> Runner:
    """Build one ref's runner on the backend it was placed on.

    With no ``placements`` every ref runs on the uncapped local host: the no-deployment
    case (tests, ``cascade run --backend``). A run with a deployment always passes them.
    """
    if placements is None or config.runner is RunnerKind.echo:
        return build_runner(config, signature=signature, cwd=cwd)
    placed = placements.backend_of(name)
    if placed is None:
        raise PlacementError([f"ref {name!r} was not placed"])
    backend = next((b for b in backends if b.name == placed), None)
    if backend is None:
        raise PlacementError([f"ref {name!r} is placed on unknown backend {placed!r}"])
    image = (images or {}).get(name)
    return build_runner(config, backend, signature=signature, cwd=cwd, image=image)


def configure_ref_builder(
    placements: PlacementMap | None = None,
    backends: Sequence[Backend] = (),
    cwd: str | None = None,
    images: Mapping[str, ImageConfig] | None = None,
) -> RefRunnerBuilder:
    """Bind the run-wide half, leaving a function of ref data alone.

    Takes the values it reads rather than a ``Deployment``: the store binding is no
    business of a runner, and ``cwd`` is not in a deployment file at all. An earlier
    version bundled them into a ``DeploymentData`` struct, which duplicated the
    deployment under a second name and mislabelled invocation context as configuration.
    """
    bound_backends = tuple(backends)
    bound_images = dict(images or {})

    def builder(name: str, config: RunConfig, signature: Signature | None = None) -> Runner:
        return build_ref(
            name, config, signature, placements=placements, backends=bound_backends,
            cwd=cwd, images=bound_images,
        )

    return builder
