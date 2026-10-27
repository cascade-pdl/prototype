"""``cascade images list`` / ``prune`` — the images cascade built, and removing old ones.

Every change to a build directory, or to cascade's source for an image that installs
it, builds a new ``cascade/<dir>:<hash>`` and leaves the previous one behind. These
commands find them by the labels set at build (the directory and its hash then), not by
name: a name is shared by every project with a directory of that name.

An image is ``current`` when its directory builds it today, ``stale`` when the directory
has changed since, ``gone`` when the directory no longer exists; ``unlabelled`` images
were built before the labels and cannot be told apart. ``prune`` removes stale and gone
ones, unlabelled ones only when asked. By default both commands look at the current
project's images (directories inside it); ``--all`` at every project's, which is safe:
an image is judged against its own directory, never against this project's pipeline.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from cascade.cli.errors import CliError
from cascade.docker.images import remove_image
from cascade.project import ProjectNotFound, find_project
from cascade.runners.images import BuiltImage, built_images

REMOVABLE = ("stale", "gone")


def _in_scope(args: argparse.Namespace) -> tuple[list[BuiltImage], Path | None]:
    """The images these commands look at, and the project root they are shown against
    (``None`` under ``--all``)."""
    found = asyncio.run(built_images())
    if args.all:
        return found, None
    try:
        _project, root = find_project(args.project)
    except ProjectNotFound:
        raise CliError("not inside a cascade project; pass --all for every project's images")
    root = root.resolve()
    return [b for b in found if b.context is not None and b.context.is_relative_to(root)], root


def _where(built: BuiltImage, root: Path | None) -> str:
    if built.context is None:
        return "?"
    if root is not None and built.context.is_relative_to(root):
        return str(built.context.relative_to(root)) or "."
    return str(built.context)


def cmd_list(args: argparse.Namespace) -> int:
    images, root = _in_scope(args)
    for built in sorted(images, key=lambda b: (str(b.context), b.status, b.names)):
        print(f"{built.status:<11} {' '.join(built.names):<40} {_where(built, root)}")
    if not images:
        print("no images built by cascade" + ("" if args.all else " in this project"))
    if not args.all:
        unlabelled = sum(b.status == "unlabelled" for b in asyncio.run(built_images()))
        if unlabelled:
            print(f"({unlabelled} image(s) built before labels: `cascade images list --all`)")
    return 0


def cmd_prune(args: argparse.Namespace) -> int:
    images, root = _in_scope(args)
    if args.unlabelled:
        images = images + [
            b for b in asyncio.run(built_images()) if b.status == "unlabelled" and b not in images
        ]
    doomed = [b for b in images if b.status in REMOVABLE or (args.unlabelled and b.status == "unlabelled")]
    if not doomed:
        print("nothing to prune")
        return 0
    for built in doomed:
        print(f"{built.status:<11} {' '.join(built.names):<40} {_where(built, root)}")
    if not args.yes:
        if not sys.stdin.isatty():
            raise CliError("not asking without a terminal: pass --yes to remove these")
        if input(f"remove {len(doomed)} image(s)? [y/N] ").strip().lower() not in ("y", "yes"):
            print("nothing removed")
            return 0
    failed = 0
    for built in doomed:
        for name in built.names:
            reason = asyncio.run(remove_image(name))
            if reason is None:
                print(f"removed {name}")
            else:
                failed += 1
                print(f"kept {name}: {reason}", file=sys.stderr)
    return 1 if failed else 0


def add_parsers(sub: argparse._SubParsersAction) -> None:
    images = sub.add_parser("images", help="the images cascade built, and removing old ones")
    images_sub = images.add_subparsers(dest="images_command", required=True)

    p = images_sub.add_parser("list", help="this project's built images and their status")
    p.add_argument("--all", action="store_true", help="every project's, not just this one's")
    p.add_argument("--project", metavar="PATH", help="start directory for cascade.toml discovery")
    p.set_defaults(func=cmd_list)

    p = images_sub.add_parser("prune", help="remove built images their directory no longer builds")
    p.add_argument("--all", action="store_true", help="every project's, not just this one's")
    p.add_argument("--unlabelled", action="store_true",
                   help="also remove cascade/* images built before labels (directory unknown)")
    p.add_argument("--yes", action="store_true", help="remove without asking")
    p.add_argument("--project", metavar="PATH", help="start directory for cascade.toml discovery")
    p.set_defaults(func=cmd_prune)
