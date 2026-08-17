"""Runner registry: turn a ``RunConfig`` into a live ``Runner``.

The counterpart to the store registry. A ``RunConfig`` is inert data carried in the
Plan — a kind plus its ``RefData`` (image, cmd) and optional ``RunnerOverrides``.
This module is where that data, the deployment's per-kind defaults, and machine-local
context meet and produce something spawnable.

**Three legs, and the precedence between them.** ``RefData`` is the author's
settled choice and is never overridden — an image is an image. ``RunnerOverrides``
exist on both the ref and the deployment, and are merged field-wise with the
**ref winning where it states a value**, the deployment supplying defaults for
fields the ref leaves unset. That reading follows ``RunConfig``'s own description
of the deployment as "defaults"; if you would rather the substrate cap what a ref
can ask for, invert ``merge_overrides`` — it is the single place that decides.

**Machine-local context is a third input, not an override.** A credentials
directory is neither an authoring choice nor a portable deployment fact; it is a
property of the machine doing the spawning. It travels in ``RunnerEnv`` rather than
being smuggled into the override vocabulary, where it would be meaningless on a ref.

No dependency on ``cascade.deployment``: callers pass the already-extracted
override for the kind (``deployment.runners.get(config.runner)``), which keeps the
engine layer below the configuration layer.
"""
from __future__ import annotations

from dataclasses import dataclass, fields, replace
from typing import Callable, Mapping

from cascade.model.runner_kinds import RunnerKind
from cascade.model.ref_data import RefData, RefDocker, RefEcho, RefSubprocess
from cascade.model.runner_overrides import RunnerOverrides
from cascade.plan.run_config import RunConfig
from cascade.plan.signature import Signature

from cascade.engine.runner.runner import Runner
from cascade.engine.runner.runner_docker import RunnerDocker
from cascade.engine.runner.runner_echo import RunnerEcho
from cascade.engine.runner.runner_subprocess import RunnerSubprocess


class RunnerKindError(Exception):
    """No builder is registered for this runner kind."""


@dataclass
class RunnerEnv:
    """Machine-local spawn context — not carried by the plan or the deployment."""

    aws_credentials_dir: str | None = None
    map_current_user: bool = True


def merge_overrides(
    ref: RunnerOverrides | None,
    deployment: RunnerOverrides | None,
) -> RunnerOverrides | None:
    """Field-wise merge; the ref wins wherever it states a value (not ``None``).

    The single point of truth for override precedence — see the module docstring.
    """
    if ref is None:
        return deployment
    if deployment is None:
        return ref
    if type(ref) is not type(deployment):
        raise RunnerKindError(
            f"cannot merge {type(ref).__name__} with {type(deployment).__name__}"
        )
    stated = {
        f.name: getattr(ref, f.name)
        for f in fields(ref)
        if getattr(ref, f.name) is not None
    }
    return replace(deployment, **stated)


def _build_docker(
    config: RefData,
    overrides: RunnerOverrides | None,
    env: RunnerEnv,
    signature: Signature | None = None,
) -> Runner:
    assert isinstance(config, RefDocker)
    no_pull = getattr(overrides, "no_pull", None)
    return RunnerDocker(
        image=config.image,
        extra_args=list(config.extra_args),
        no_pull=True if no_pull is None else no_pull,
        map_current_user=env.map_current_user,
        aws_credentials_dir=env.aws_credentials_dir,
        memory=getattr(overrides, "memory", None),
        cpu=getattr(overrides, "cpu", None),
    )


def _build_subprocess(
    config: RefData,
    overrides: RunnerOverrides | None,
    env: RunnerEnv,
    signature: Signature | None = None,
) -> Runner:
    assert isinstance(config, RefSubprocess)
    return RunnerSubprocess(cmd=list(config.cmd))


def _build_echo(
    config: RefData,
    overrides: RunnerOverrides | None,
    env: RunnerEnv,
    signature: Signature | None = None,
) -> Runner:
    assert isinstance(config, RefEcho)
    # output ports come from the plan: per-runnable, like the runner itself
    return RunnerEcho(
        message=config.message,
        # port -> declared depth, so a T[] stub is a list
        outputs={p: t.depth for p, t in signature.outputs.items()} if signature else {},
    )


Builder = Callable[..., Runner]

RUNNER_BUILDERS: Mapping[RunnerKind, Builder] = {
    RunnerKind.docker: _build_docker,
    RunnerKind.subprocess: _build_subprocess,
    RunnerKind.echo: _build_echo,
}


def build_runner(
    config: RunConfig,
    deployment_overrides: RunnerOverrides | None = None,
    env: RunnerEnv | None = None,
    signature: Signature | None = None,
) -> Runner:
    """Build the live runner for one runnable.

    ``deployment_overrides`` is the deployment's entry for this kind, if any
    (``deployment.runners.get(config.runner)``). ``signature`` is the runnable's
    entry in ``plan.signatures``; kinds that need to know their own ports (echo,
    writing stubs) use it, others ignore it. It is passed here rather than per spawn
    because a signature is per-runnable, not per-instance.
    """
    try:
        build = RUNNER_BUILDERS[config.runner]
    except KeyError:
        raise RunnerKindError(f"no runner registered for kind {config.runner.value!r}")
    return build(
        config.config,
        merge_overrides(config.overrides, deployment_overrides),
        env or RunnerEnv(),
        signature=signature,
    )