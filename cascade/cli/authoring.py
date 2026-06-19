"""CLI wiring for the `authoring` namespace (new, list-refs, suggest-image-name).
The command logic lives in cascade.provisioning (importable/testable without the
CLI); this module just registers the subparsers."""

from __future__ import annotations

from ..provisioning import add_authoring_subcommands


def add_subcommands(sub):
    add_authoring_subcommands(sub)
