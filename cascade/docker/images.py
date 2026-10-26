"""Images: inspect, check, pull, build."""
from __future__ import annotations

import asyncio
import sys
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from cascade.docker.cli import docker, docker_json


class ImageNotFound(Exception):
    """The image is not present locally (and was not, or could not be, pulled)."""


class BuildFailed(Exception):
    """``docker build`` exited non-zero."""

    def __init__(self, code: int, tail: str):
        super().__init__(f"docker exit {code}")
        self.code = code
        self.tail = tail
        """The last lines of its output."""


@dataclass(frozen=True)
class ImageConfig:
    """What a wrapped run needs from an image's own config.

    Read from the image because overriding the entrypoint (to put cascade first) discards
    the image's own; it is passed on after ``--`` instead. ``id`` is what is run, so the
    container is exactly the image that was inspected, even if its tag moves meanwhile.
    """

    id: str
    entrypoint: tuple[str, ...]
    cmd: tuple[str, ...]


async def inspect_image(image: str, pull: bool) -> ImageConfig:
    """The image's id, entrypoint and default command, pulling it first if allowed."""
    raw = await docker_json("image", "inspect", image)
    if raw is None and pull:
        await docker("pull", "--quiet", image)
        raw = await docker_json("image", "inspect", image)
    if raw is None:
        raise ImageNotFound(image)
    config = raw[0].get("Config") or {}
    return ImageConfig(
        id=raw[0]["Id"],
        entrypoint=tuple(config.get("Entrypoint") or ()),
        cmd=tuple(config.get("Cmd") or ()),
    )


async def image_exists(image: str) -> bool:
    """Whether ``image`` is present locally."""
    code, _, _ = await docker("image", "inspect", image)
    return code == 0


async def build_image(
    tag: str, context: Path, build_contexts: Mapping[str, Path] | None = None
) -> None:
    """``docker build`` of ``context`` into ``tag``, its output passed through to stderr.

    ``build_contexts`` are BuildKit named contexts (``COPY --from=<name>``)."""
    argv = ["docker", "build", "--tag", tag]
    for name, path in (build_contexts or {}).items():
        argv += ["--build-context", f"{name}={path}"]
    argv.append(str(context))
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
        raise BuildFailed(code, "".join(tail))
