"""Where cascade sits inside a container, and how a runner asks it to wrap a command.

A ref's image needs no cascade inside. For a *wrapped* ref the runner supplies cascade
from outside the image, as a self-contained bundle (``bundle/build.sh``) at
``BUNDLE_ROOT``, makes ``BUNDLE_ENTRYPOINT`` the container's entrypoint, and passes
``wrap_args(command)``: the bundle's ``cascade node wrap``, then the image's own command.
How the bundle gets there is the backend's business (mounted locally, a layer on ECS);
these names are the part every backend shares.

The node side of the same agreement is ``cascade.node.wrap``: stage inputs under
``STAGING_ROOT/inputs``, run the command, collect ``STAGING_ROOT/outputs``.
"""
from __future__ import annotations

from typing import Sequence


BUNDLE_ROOT = "/opt/cascade"
"""Where the bundle is inside a container."""

BUNDLE_ENTRYPOINT = f"{BUNDLE_ROOT}/bin/cascade"
"""The bundle's ``cascade`` command. Started by the kernel through its shebang, so the
image needs no shell."""

STAGING_ROOT = "/cascade"
"""Where a wrapped tool finds ``inputs/<port>`` and writes ``outputs/<port>`` (the node
SDK's default ``CASCADE_ROOT``)."""


def wrap_args(command: Sequence[str]) -> list[str]:
    """Arguments to ``cascade`` that run ``command`` wrapped: ``node wrap -- <command>``.

    The staging root comes from ``CASCADE_ROOT`` (default ``STAGING_ROOT``), so a host
    runner gives each instance its own by setting that variable.
    """
    if not command:
        raise ValueError("nothing to wrap: the command is empty")
    return ["node", "wrap", "--", *command]
