"""Running the ``docker`` client."""
from __future__ import annotations

import asyncio
import json
from typing import Any


async def docker(*argv: str) -> tuple[int, bytes, bytes]:
    """Run ``docker <argv>``; its exit code, stdout and stderr."""
    proc = await asyncio.create_subprocess_exec(
        "docker", *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    out, err = await proc.communicate()
    return proc.returncode, out, err


async def docker_json(*argv: str) -> Any:
    """The parsed JSON output of ``docker <argv>``, or ``None`` if it failed."""
    code, out, _ = await docker(*argv)
    return json.loads(out) if code == 0 else None
