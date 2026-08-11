"""``cascade new`` — scaffold an empty project.

Writes the three files a project is made of: ``cascade.toml`` (identity),
``pipeline.yaml`` (what to run), ``deployment.yaml`` (what to run it on).

Prompting is **opt-out by circumstance, not by flag**: metadata is asked for only
when a value was not passed *and* stdin is a terminal. Under CI, a pipe, or
``--yes`` the prompts are skipped and the fields are simply left unset, so this
command can never hang a script.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from cascade.project import Project, PROJECT_FILE, find_project_file, ProjectNotFound
from cascade.cli.errors import CliError
from cascade.cli.templates import PIPELINE_SKELETON, DEPLOYMENT_SKELETON


def _interactive(args: argparse.Namespace) -> bool:
    return not args.yes and sys.stdin.isatty()


def _ask(prompt: str) -> str | None:
    """Ask for an optional value; empty input means "leave unset"."""
    try:
        answer = input(f"{prompt} (optional, enter to skip): ").strip()
    except (EOFError, KeyboardInterrupt):
        print()
        return None
    return answer or None


def _prepare_target(path: Path) -> None:
    """Create the target directory, or verify an existing one is empty."""
    if path.exists():
        if not path.is_dir():
            raise CliError(f"target exists and is not a directory: {path}")
        if any(path.iterdir()):
            raise CliError(f"target directory is not empty: {path}")
        return
    try:
        path.mkdir(parents=True)
    except OSError as e:
        raise CliError(f"could not create {path}: {e}")


def cmd_new(args: argparse.Namespace) -> int:
    name = args.name.strip()
    if not name:
        raise CliError("project name must not be empty")

    target = Path(args.path) if args.path else Path(name)
    _prepare_target(target)

    # a project inside another project is legal (monorepos) but usually a slip
    try:
        enclosing = find_project_file(target.parent)
        print(f"note: this project sits inside another one ({enclosing})")
    except ProjectNotFound:
        pass

    description = args.description
    email = args.email
    if _interactive(args):
        if description is None:
            description = _ask("description")
        if email is None:
            email = _ask("contact email")

    project = Project(name=name, description=description, email=email)
    project.save(target / PROJECT_FILE)
    (target / project.pipeline).write_text(PIPELINE_SKELETON)
    (target / project.deployment).write_text(DEPLOYMENT_SKELETON)

    print(f"created project {name!r} in {target}")
    for f in (PROJECT_FILE, project.pipeline, project.deployment):
        print(f"  {f}")
    print("\nnext: define a ref and a dag in "
          f"{project.pipeline}, then run `cascade validate {project.pipeline}`")
    return 0


def add_parsers(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("new", help="scaffold an empty project")
    p.add_argument("name", help="project name")
    p.add_argument("path", nargs="?",
                   help="target directory (default: a new directory named NAME)")
    p.add_argument("--description", help="project description")
    p.add_argument("--email", help="contact email")
    p.add_argument("-y", "--yes", action="store_true",
                   help="never prompt; leave unsupplied metadata unset")
    p.set_defaults(func=cmd_new)