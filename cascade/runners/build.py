"""Turning configuration into runners — the one place that knows how.

Two stages, because two different things vary at two different times:

- **run-wide settings** are fixed for a whole run: the deployment's per-kind overrides,
  and where a relative command resolves. Bound once, by ``configure_ref_builder``.
- **ref data** varies per node: the ref's own config and its resolved ports. Passed at
  call time.

The result is a closure carrying those two values and nothing else — no plan, no
pipeline, no names, and no ``Deployment``: a runner has no business with a store binding,
and ``cwd`` is not in a deployment file at all.

``DagRunner`` used to carry a ``RunnerEnv`` and an overrides map purely to hand them to
``build_runner``: it stored both, threaded both through its recursion, and read neither.
Coordination carrying configuration through itself is what made the packages tangle.

**Refs only.** ``RunnerKind`` has no ``dag`` member, and a ``DagRunner`` is built from a
plan rather than from a ref's config — so the two are not alternatives, and a builder able
to return either would be conflating substrate with coordination. ``DagRunner`` keeps its
own recursion and asks this only for refs.
"""
from __future__ import annotations

from typing import Callable, Mapping

from cascade.model.runner_kinds import RunnerKind
from cascade.model.runner_overrides import RunnerOverrides
from cascade.plan.run_config import RunConfig
from cascade.plan.signature import Signature
from cascade.protocol.runner import Runner
from cascade.runners.registry import build_runner


RefRunnerBuilder = Callable[[RunConfig, "Signature | None"], Runner]
"""Ref data in, runner out. The whole seam between coordination and substrate."""


def build_ref(
    config: RunConfig,
    signature: Signature | None = None,
    *,
    overrides: Mapping[RunnerKind, RunnerOverrides] | None = None,
    cwd: str | None = None,
) -> Runner:
    """Build one ref's runner from its own data and the run-wide settings."""
    return build_runner(
        config,
        deployment_overrides=(overrides or {}).get(config.runner),
        signature=signature,
        cwd=cwd,
    )


def configure_ref_builder(
    overrides: Mapping[RunnerKind, RunnerOverrides] | None = None,
    cwd: str | None = None,
) -> RefRunnerBuilder:
    """Bind the run-wide half, leaving a function of ref data alone.

    Takes the two values it reads rather than a ``Deployment``: the store binding is no
    business of a runner, and ``cwd`` is not in a deployment file at all. An earlier
    version bundled them into a ``DeploymentData`` struct, which duplicated
    ``Deployment.runners`` under a second name and mislabelled invocation context as
    configuration.
    """
    bound_overrides = dict(overrides or {})

    def builder(config: RunConfig, signature: Signature | None = None) -> Runner:
        return build_ref(config, signature, overrides=bound_overrides, cwd=cwd)

    return builder
