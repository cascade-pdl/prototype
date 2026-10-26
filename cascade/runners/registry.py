"""Runner registry: turn a ``RunConfig`` and its backend into a live ``Runner``.

The counterpart to the store registry. A ``RunConfig`` is inert data carried in the
Plan — the ref's ``RefData`` (image, cmd; its kind derives from it) and the resources it
needs. A ``Backend`` is where placement put it. This module is where the two
meet and produce something spawnable.

**Two inputs, no precedence between them.** They say different things, so nothing is
merged: the ref says *what* runs and how big it is (an image is an image, and the
resources are the allocation as well as the minimum); the backend says *where* and with
which machine settings (credentials, user mapping, pull policy). The per-kind override
layer that used to merge ref and deployment defaults is gone — sizes moved to the ref's
``resources``, machine settings to the backend's ``config``.

The builder is chosen by the pair (backend kind, ref kind): local docker and ECS run the
same docker ref differently. ``echo`` refs are stubs and are never placed, so they are
built without a backend.

No dependency on ``cascade.deployment``: callers pass the ``Backend`` itself, which keeps
the substrate layer below the configuration layer.
"""
from __future__ import annotations

import os

from typing import Callable, Mapping

from cascade.model.runner_kinds import RunnerKind
from cascade.model.ref_data import RefDocker, RefEcho, RefSubprocess
from cascade.plan.run_config import RunConfig
from cascade.plan.signature import Signature
from cascade.substrate.backends import Backend, BackendKind, LocalConfig, RUNS, uncapped_local

from cascade.protocol.runner import Runner
from cascade.runners.docker import ImageConfig, RunnerDocker
from cascade.runners.images import image_of
from cascade.runners.echo import RunnerEcho
from cascade.runners.process import RunnerSubprocess


class RunnerKindError(Exception):
    """No builder is registered for this pairing of backend and ref kind."""


class BundleError(Exception):
    """A wrapped ref's backend points at no usable cascade bundle."""


def _build_local_docker(
    config: RunConfig,
    backend: Backend,
    signature: Signature | None = None,
    cwd: str | None = None,
    image: ImageConfig | None = None,
) -> Runner:
    ref, local = config.config, backend.config
    assert isinstance(ref, RefDocker) and isinstance(local, LocalConfig)
    resources = config.resources
    return RunnerDocker(
        image=image_of(ref),
        resolved=image,
        extra_args=list(ref.extra_args),
        # a built image exists only here, made before the run: there is nowhere to pull
        # it from
        no_pull=local.no_pull or ref.build is not None,
        map_current_user=local.map_current_user,
        aws_credentials_dir=local.aws_credentials_dir,
        memory=resources.memory if resources else None,
        cpu=resources.cpu if resources else None,
        gpu=resources.gpu if resources else 0,
        command=None if ref.command is None else list(ref.command),
        bundle=_bundle_path(backend, cwd) if ref.wrap else None,
    )


def _bundle_path(backend: Backend, cwd: str | None) -> str:
    """The backend's bundle as an absolute host path, checked to be a bundle.

    Placement has already refused a wrapped ref on a backend with no ``bundle``; this
    checks what placement cannot, the path itself, which is relative to the pipeline's
    directory."""
    local = backend.config
    assert isinstance(local, LocalConfig)
    if local.bundle is None:
        raise BundleError(
            f"backend {backend.name!r} has no `bundle` to wrap docker refs with; set one, "
            "or `wrap: false` on refs whose image has cascade inside"
        )
    path = os.path.abspath(os.path.join(cwd or os.getcwd(), local.bundle))
    if not os.path.isfile(os.path.join(path, "bin", "cascade")):
        raise BundleError(
            f"backend {backend.name!r}: no cascade bundle at {path} (from `bundle: "
            f"{local.bundle}`); build one with bundle/build.sh"
        )
    return path


def _build_local_subprocess(
    config: RunConfig,
    backend: Backend,
    signature: Signature | None = None,
    cwd: str | None = None,
    image: ImageConfig | None = None,
) -> Runner:
    # resources are matched and counted, not enforced: a plain process has no cap
    ref = config.config
    assert isinstance(ref, RefSubprocess)
    return RunnerSubprocess(cmd=list(ref.cmd), cwd=cwd, wrap=ref.wrap)


def _build_ecs_docker(
    config: RunConfig,
    backend: Backend,
    signature: Signature | None = None,
    cwd: str | None = None,
    image: ImageConfig | None = None,
) -> Runner:
    raise RunnerKindError(
        f"backend {backend.name!r} is ECS, and the ECS runner is not built yet"
    )


def _build_echo(config: RunConfig, signature: Signature | None = None) -> Runner:
    ref = config.config
    assert isinstance(ref, RefEcho)
    # output ports come from the plan: per-runnable, like the runner itself
    return RunnerEcho(
        message=ref.message,
        # port -> declared depth, so a T[] stub is a list
        outputs={p: d.type.depth for p, d in signature.outputs.items()} if signature else {},
    )


Builder = Callable[..., Runner]

RUNNER_BUILDERS: Mapping[tuple[BackendKind, RunnerKind], Builder] = {
    (BackendKind.local, RunnerKind.docker): _build_local_docker,
    (BackendKind.local, RunnerKind.subprocess): _build_local_subprocess,
    (BackendKind.ecs, RunnerKind.docker): _build_ecs_docker,
}
assert set(RUNNER_BUILDERS) == {(b, r) for b, kinds in RUNS.items() for r in kinds}, (
    "every pairing placement allows needs a builder"
)


def build_runner(
    config: RunConfig,
    backend: Backend | None = None,
    signature: Signature | None = None,
    cwd: str | None = None,
    image: ImageConfig | None = None,
) -> Runner:
    """Build the live runner for one ref on the backend it was placed on.

    ``backend`` defaults to the uncapped local host, which is what a run with no
    deployment uses. ``signature`` is the runnable's entry in ``plan.signatures``; kinds
    that need to know their own ports (echo, writing stubs) use it, others ignore it. It
    is passed here rather than per spawn because a signature is per-runnable, not
    per-instance.
    """
    if config.runner is RunnerKind.echo:
        return _build_echo(config, signature)
    backend = backend or uncapped_local()
    try:
        build = RUNNER_BUILDERS[(backend.kind, config.runner)]
    except KeyError:
        raise RunnerKindError(
            f"a {backend.kind.value} backend ({backend.name!r}) cannot run a "
            f"{config.runner.value} ref"
        )
    return build(config, backend, signature=signature, cwd=cwd, image=image)
