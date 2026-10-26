"""Containers, by name."""
from __future__ import annotations

import asyncio


async def stop(name: str, grace: float) -> bool:
    """``docker stop`` with ``grace`` seconds before the kill; false if there was no such
    container (not created yet, or already gone)."""
    proc = await asyncio.create_subprocess_exec(
        "docker", "stop", "--time", str(int(grace)), name,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    return await proc.wait() == 0
