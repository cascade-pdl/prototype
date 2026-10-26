"""Turning configuration into runners — the one place that knows how.

Two stages, because two different things vary at two different times:

- **run-wide settings** are fixed for a whole run: where each ref was placed, the backends
  it was placed on, where a relative command resolves, each docker ref's image as
  resolved for the run, where its containers fetch credentials, and the run's
  concurrency limits. Bound once, by
  ``configure_ref_builder``.
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

**Limits are bound here too.** A limit is run-wide, and this builder is the one thing every
leaf of the run passes through, while one runner is built per dispatch: so the builder
holds the run's one ``Limiter`` and wraps each leaf that has a pool in a
``LimitedRunner``. Coordination never sees it.

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
from cascade.docker.images import ImageConfig
from cascade.runners.credentials import ContainerCredentials
from cascade.runners.limiter import Limiter, LimitedRunner, Pool, backend_pool, named_pool
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
    limiter: Limiter | None = None,
    credentials: ContainerCredentials | None = None,
) -> Runner:
    """Build one ref's runner on the backend it was placed on, holding its slots in
    ``limiter`` while it runs.

    With no ``placements`` every ref runs on the uncapped local host (tests, ``cascade
    run --backend``): no backend pool, but a ref's named limit still holds, since it is
    part of what the pipeline means. A run with a deployment always passes them.
    """
    if config.runner is RunnerKind.echo:
        return build_runner(config, signature=signature, cwd=cwd)
    if placements is None:
        runner = build_runner(config, signature=signature, cwd=cwd, credentials=credentials)
        return _limited(runner, _named_pools(config, limiter), limiter)
    placed = placements.backend_of(name)
    if placed is None:
        raise PlacementError([f"ref {name!r} was not placed"])
    backend = next((b for b in backends if b.name == placed), None)
    if backend is None:
        raise PlacementError([f"ref {name!r} is placed on unknown backend {placed!r}"])
    image = (images or {}).get(name)
    runner = build_runner(
        config, backend, signature=signature, cwd=cwd, image=image, credentials=credentials
    )
    backend_pools = (backend_pool(backend.name),) if backend.concurrency is not None else ()
    return _limited(runner, backend_pools + _named_pools(config, limiter), limiter)


def _limited(runner: Runner, pools: tuple[Pool, ...], limiter: Limiter | None) -> Runner:
    return LimitedRunner(runner, limiter, pools) if limiter and pools else runner


def _named_pools(config: RunConfig, limiter: Limiter | None) -> tuple[Pool, ...]:
    """The ref's named pool, if it names one. A label the limiter does not know is an
    error, never "unlimited" (compile reports it first)."""
    if config.limit is None:
        return ()
    pool = named_pool(config.limit)
    if limiter is None or limiter.capacity(pool) is None:
        raise PlacementError([f"limit {config.limit!r} has no capacity in the plan"])
    return (pool,)


def run_limiter(backends: Sequence[Backend], limits: Mapping[str, int]) -> Limiter:
    """The pools for one run, each from exactly one source: each capped backend from the
    deployment, each named limit from the plan. The two kinds share no namespace."""
    capacities = {backend_pool(b.name): b.concurrency for b in backends if b.concurrency}
    capacities.update({named_pool(label): n for label, n in limits.items()})
    return Limiter(capacities)


def configure_ref_builder(
    placements: PlacementMap | None = None,
    backends: Sequence[Backend] = (),
    cwd: str | None = None,
    images: Mapping[str, ImageConfig] | None = None,
    limits: Mapping[str, int] | None = None,
    credentials: ContainerCredentials | None = None,
) -> RefRunnerBuilder:
    """Bind the run-wide half, leaving a function of ref data alone.

    One ``Limiter`` per builder, so one per run: backends' ``concurrency`` and the
    plan's named ``limits`` count across the whole run. Build a new builder per run.

    Takes the values it reads rather than a ``Deployment``: the store binding is no
    business of a runner, and ``cwd`` is not in a deployment file at all. An earlier
    version bundled them into a ``DeploymentData`` struct, which duplicated the
    deployment under a second name and mislabelled invocation context as configuration.
    """
    bound_backends = tuple(backends)
    bound_images = dict(images or {})
    limiter = run_limiter(bound_backends, limits or {})

    def builder(name: str, config: RunConfig, signature: Signature | None = None) -> Runner:
        return build_ref(
            name, config, signature, placements=placements, backends=bound_backends,
            cwd=cwd, images=bound_images, limiter=limiter, credentials=credentials,
        )

    return builder
