"""The images docker refs run: built and resolved once per run, before it starts.

Each built image is named by the content hash compile recorded in the plan
(``cascade.substrate.naming.built_image``), so preparing is "build every name that does
not exist yet". Done once, up front, by the executor (and by ``cascade build`` on its
own): a Dockerfile that does not build fails the run before any task starts, and refs
that share a build directory share one build.

The directory is hashed again first. A plan compiled before the directory changed names
an image that would be built from different content, so that is refused rather than
built under a name that no longer describes it.

Then every docker ref's image, built or named, is **resolved once** (``resolve_images``):
its id, entrypoint and default command, pulled first if the backend allows. Runners are
handed that, so a fan of a thousand lanes asks Docker nothing per instance, and every
instance of a ref runs the identical image even if its tag moves during the run.
"""
from __future__ import annotations

import asyncio
import os
import shutil
import sys
import tempfile
from collections import deque
from pathlib import Path
from typing import Sequence

from cascade.model.ref_data import RefDocker
from cascade.plan.build_context import (
    CASCADE_CONTEXT,
    cascade_source,
    check_context,
    context_hash,
    uses_cascade,
)
from cascade.plan.plan import Plan
from cascade.runners.docker import DockerImageError, ImageConfig, image_exists, inspect_image
from cascade.substrate.backends import Backend, BackendKind, LocalConfig
from cascade.substrate.naming import built_image
from cascade.substrate.placement import PlacementMap


class ImageBuildError(Exception):
    """An image a ``build:`` ref needs could not be built."""


async def prepare_images(
    plan: Plan,
    placements: PlacementMap,
    backends: Sequence[Backend],
    cwd: str | None = None,
) -> list[str]:
    """Build every missing image a placed ``build:`` ref needs; return the names built.

    ``cwd`` is the pipeline's directory, which ``build:`` paths are relative to.
    """
    by_name = {b.name: b for b in backends}
    wanted: dict[str, Path] = {}
    for placement in placements:
        ref = plan.run_config[placement.ref].config
        if not isinstance(ref, RefDocker) or ref.build is None:
            continue
        backend = by_name[placement.backend]
        if backend.kind is not BackendKind.local:
            raise ImageBuildError(
                f"ref {placement.ref!r} builds its image, which only local backends do so "
                f"far ({backend.name!r} is {backend.kind.value})"
            )
        context = Path(cwd or os.getcwd(), ref.build).resolve()
        if problems := check_context(context):
            raise ImageBuildError(f"ref {placement.ref!r}: {'; '.join(problems)}")
        if context_hash(context) != ref.build_hash:
            raise ImageBuildError(
                f"ref {placement.ref!r}: {ref.build} has changed since the plan was "
                "compiled; compile it again"
            )
        wanted.setdefault(built_image(ref.build, ref.build_hash), context)

    built = []
    for image, context in wanted.items():
        if not await image_exists(image):
            await _build(image, context)
            built.append(image)
    return built


def image_of(ref: RefDocker) -> str:
    """The image a docker ref runs: as named, or the one built from its directory."""
    if ref.image is not None:
        return ref.image
    if not ref.build_hash:
        raise ValueError(
            f"a ref builds from {ref.build!r} but has no build hash: compile it from its "
            "pipeline file, which hashes the directory"
        )
    return built_image(ref.build, ref.build_hash)


async def resolve_images(
    plan: Plan, placements: PlacementMap, backends: Sequence[Backend]
) -> dict[str, ImageConfig]:
    """Each locally placed docker ref's image, inspected once: ref name -> config.

    One inspection per distinct image, however many refs or instances use it. A missing
    image is pulled when the backend allows it (never for a built one, which exists only
    here) and is otherwise an error now, before any task starts.
    """
    by_name = {b.name: b for b in backends}
    pull: dict[str, bool] = {}
    image_for: dict[str, str] = {}
    for placement in placements:
        ref = plan.run_config[placement.ref].config
        local = by_name[placement.backend].config
        if not isinstance(ref, RefDocker) or not isinstance(local, LocalConfig):
            continue
        image = image_of(ref)
        image_for[placement.ref] = image
        allowed = ref.build is None and not local.no_pull
        pull[image] = pull.get(image, False) or allowed

    resolved: dict[str, ImageConfig] = {}
    for image, allowed in pull.items():
        try:
            resolved[image] = await inspect_image(image, pull=allowed)
        except DockerImageError as e:
            users = sorted(r for r, i in image_for.items() if i == image)
            raise ImageBuildError(f"ref(s) {users}: {e}")
    return {ref: resolved[image] for ref, image in image_for.items()}


async def _build(image: str, context: Path) -> None:
    """``docker build`` into ``image``, its output passed through to stderr."""
    with tempfile.TemporaryDirectory(prefix="cascade-source-") as staged:
        argv = ["docker", "build", "--tag", image]
        if uses_cascade(context):
            for name, source in cascade_source():
                target = Path(staged, name)
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, target)
            argv += ["--build-context", f"{CASCADE_CONTEXT}={staged}"]
        argv.append(str(context))

        print(f"cascade: building {image} from {context}", file=sys.stderr, flush=True)
        proc = await asyncio.create_subprocess_exec(
            *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
        )
        tail: deque[str] = deque(maxlen=15)
        assert proc.stdout is not None
        async for line in proc.stdout:
            text = line.decode(errors="replace")
            sys.stderr.write(text)
            tail.append(text)
        if code := await proc.wait():
            raise ImageBuildError(
                f"building {image} from {context} failed (docker exit {code}):\n"
                + "".join(tail)
            )
