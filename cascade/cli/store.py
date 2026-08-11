"""Store commands: move data in and out of the resolved store. Unlike the author
commands these are project-aware — they build a live store via ``_resolve`` (from
the deployment, or from ``--backend`` flags) before doing anything."""
from __future__ import annotations

import argparse
from pathlib import Path

from cascade.cli.errors import CliError
from cascade.cli._resolve import add_store_flags, build_store, at_tuple


def cmd_stage(args: argparse.Namespace) -> int:
    src = Path(args.file)
    if not src.is_file():
        raise CliError(f"not a file: {src}")
    store = build_store(args)
    store.put(args.key, src.read_bytes(), at=at_tuple(args))
    print(f"staged {src} -> {args.key}")
    return 0


def cmd_fetch(args: argparse.Namespace) -> int:
    store = build_store(args)
    at = at_tuple(args)
    if not store.has(args.key, at=at):
        raise CliError(f"key not found in store: {args.key}")
    dst = Path(args.dest)
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_bytes(store.get(args.key, at=at))
    print(f"fetched {args.key} -> {dst}")
    return 0


def cmd_stage_dir(args: argparse.Namespace) -> int:
    root = Path(args.dir)
    if not root.is_dir():
        raise CliError(f"not a directory: {root}")
    store = build_store(args)
    at = at_tuple(args)
    files = sorted(p for p in root.rglob("*") if p.is_file())
    if not files:
        raise CliError(f"no files under {root}")
    for f in files:
        key = f.relative_to(root).as_posix()  # posix keys, stable across OSes
        store.put(key, f.read_bytes(), at=at)
    print(f"staged {len(files)} file(s) from {root}")
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    keys = build_store(args).list(at=at_tuple(args))
    for k in sorted(keys):
        print(k)
    return 0


def add_parsers(sub: argparse._SubParsersAction) -> None:
    store = sub.add_parser("store", help="stage and fetch data in the store")
    store_sub = store.add_subparsers(dest="store_command", required=True)

    p = store_sub.add_parser("stage", help="put a local file into the store")
    p.add_argument("key", help="store key")
    p.add_argument("file", help="local file to stage")
    p.add_argument("--at", nargs="*", metavar="SEG", help="descend fragments")
    add_store_flags(p)
    p.set_defaults(func=cmd_stage)

    p = store_sub.add_parser("fetch", help="get a key from the store to a local file")
    p.add_argument("key", help="store key")
    p.add_argument("dest", help="local destination path")
    p.add_argument("--at", nargs="*", metavar="SEG", help="descend fragments")
    add_store_flags(p)
    p.set_defaults(func=cmd_fetch)

    p = store_sub.add_parser("stage-dir", help="stage every file under a directory")
    p.add_argument("dir", help="local directory to stage")
    p.add_argument("--at", nargs="*", metavar="SEG", help="descend fragments")
    add_store_flags(p)
    p.set_defaults(func=cmd_stage_dir)

    p = store_sub.add_parser("list", help="list keys under a scope")
    p.add_argument("--at", nargs="*", metavar="SEG", help="descend fragments")
    add_store_flags(p)
    p.set_defaults(func=cmd_list)