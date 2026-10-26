"""Derived names: one source of truth, so what creates an artifact and what uses it agree.

An image cascade builds, and later an ECR repository or a task definition, is created by
one part of cascade and referenced by another. Computing the name in two places would
drift, and the failure is opaque: a run asks for an image nobody built. Every derived name
comes from here.

Only names cascade *derives* live here. An image a ref names (``image:``) is used exactly
as written; a third-party image is whatever its publisher called it.

Built images are named by the content hash of their build directory, so a name changes
exactly when what goes into the image does, "is it built?" is "does the name exist?", and
the same directory gives the same name on every machine. The project is not part of a
local name: the hash already makes it unique, and a run may have no project at all
(``cascade run --backend``).

Conventions for the ECS work, recovered from the design chat (``ecr repository
<project>/<ref>``, ``taskdef family <project>-<ref>``, ``container name <ref>`` which must
equal the ``containerOverrides`` key, ``log group /ecs/<project>`` with stream prefix
``<ref>``), join this module when that code does.
"""
from __future__ import annotations

import re
from pathlib import PurePosixPath

HASH_LENGTH = 16
"""Hex digits of the content hash in a tag: 64 bits, ample for one project's builds."""


def built_image(build: str, build_hash: str) -> str:
    """The local name of the image built from ``build``: ``cascade/<dir name>:<hash>``.

    Refs that build from the same directory share the image.
    """
    name = re.sub(r"[^a-z0-9._-]+", "-", PurePosixPath(build).name.lower()).strip("-._")
    return f"cascade/{name or 'image'}:{build_hash[:HASH_LENGTH]}"
