"""CLI wiring for the `provisioning` namespace (docker, ecs-task). The command
logic and pure helpers (build_taskdef, naming) live in cascade.provisioning;
this module just registers the subparsers."""

from __future__ import annotations

from ..provisioning import add_provisioning_subcommands


def add_subcommands(sub):
    add_provisioning_subcommands(sub)
