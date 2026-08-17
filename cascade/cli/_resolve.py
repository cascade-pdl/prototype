"""Resolve a live :class:`~cascade.store.base.Store` for the store subcommands.

The precedence we settled on: **flags override deployment override nothing.**

- ``--backend`` given  -> build the store entirely from flags (an explicit backend
  choice; the project's deployment is not consulted).
- otherwise            -> discover the project (``cascade.toml``), load its
  deployment, and use ``deployment.store``.
- neither              -> a hard error. No silent fallback to some default store
  location: staging into a surprise directory is worse than failing loudly.

``--scope`` is the one universal override (every ``StoreConfig`` has a scope), and
it is applied on top of whichever base config the rules above produced.

This lives in the library-facing CLI layer, not inside a command, because the
eventual executor resolves a store the same way — one resolver, no drift.
"""
from __future__ import annotations

import argparse
from dataclasses import replace

from cascade.project import find_project, ProjectNotFound
from cascade.deployment import Deployment
from pathlib import Path

from cascade.store.base import Store, StoreConfig
from cascade.store.file_store import FileConfig
from cascade.store.s3_store import S3Config
from cascade.store.registry import decode as build_live_store, encode_config

from cascade.cli.errors import CliError


def add_store_flags(parser: argparse.ArgumentParser) -> None:
    """Attach the store-resolution flags shared by every store subcommand."""
    g = parser.add_argument_group("store resolution")
    g.add_argument("--project", metavar="PATH",
                   help="start directory for cascade.toml discovery (default: cwd)")
    g.add_argument("--deployment", metavar="PATH",
                   help="deployment file to load (default: the project's pointer)")
    g.add_argument("--backend", choices=["file", "s3"],
                   help="build the store from flags instead of the deployment")
    g.add_argument("--root", metavar="PATH", help="[file backend] store root directory")
    g.add_argument("--bucket", help="[s3 backend] bucket name")
    g.add_argument("--prefix", default="", help="[s3 backend] key prefix")
    g.add_argument("--region", help="[s3 backend] AWS region")
    g.add_argument("--scope", nargs="*", metavar="SEG",
                   help="override the base scope tuple (applies to any backend)")


def _config_from_flags(args: argparse.Namespace) -> StoreConfig:
    if args.backend == "file":
        if not args.root:
            raise CliError("--backend file requires --root")
        return FileConfig(root=args.root)
    if args.backend == "s3":
        if not args.bucket:
            raise CliError("--backend s3 requires --bucket")
        return S3Config(bucket=args.bucket, prefix=args.prefix, region=args.region)
    raise CliError(f"unknown backend {args.backend!r}")


def _project_root(args: argparse.Namespace) -> Path:
    try:
        _project, root = find_project(args.project)
    except ProjectNotFound:
        raise CliError(
            "not inside a cascade project; pass --backend/--root or cd into one"
        )
    return root


def _config_from_project(args: argparse.Namespace) -> StoreConfig:
    try:
        project, root = find_project(args.project)
    except ProjectNotFound:
        raise CliError(
            "not inside a cascade project; pass --backend/--root or cd into one"
        )
    dep_path = args.deployment or project.deployment_file(root)
    try:
        deployment = Deployment.load(dep_path)
    except FileNotFoundError:
        raise CliError(f"deployment file not found: {dep_path}")
    return deployment.store


def _absolute_root(config: StoreConfig, base: Path) -> StoreConfig:
    """Resolve a relative file-store root against ``base``.

    A relative root is ambiguous the moment anything runs in a different working
    directory — and a subprocess ref does exactly that, since its command resolves
    against the pipeline's directory. Absolute here means parent and child agree, and
    the config that travels in ``CASCADE_STORE_OUT`` names one place.
    """
    if isinstance(config, FileConfig) and not Path(config.root).is_absolute():
        return replace(config, root=str((base / config.root).resolve()))
    return config


def build_store(args: argparse.Namespace) -> Store:
    """Resolve args to a live store, applying the precedence rules above."""
    if args.backend:
        # a flag-supplied root is relative to where the operator typed it
        config = _absolute_root(_config_from_flags(args), Path.cwd())
    else:
        project_root = _project_root(args)
        # a deployment-supplied root is relative to the project that declared it
        config = _absolute_root(_config_from_project(args), project_root)
    if args.scope is not None:
        config = replace(config, scope=tuple(args.scope))
    # go through the registry envelope so this is exactly the store a node would
    # reconstruct from CASCADE_STORE_CONF — same bytes, same object
    return build_live_store(encode_config(config))


def at_tuple(args: argparse.Namespace) -> tuple[str, ...]:
    """The ``--at`` descend fragments as a tuple (empty when not given)."""
    return tuple(getattr(args, "at", None) or ())