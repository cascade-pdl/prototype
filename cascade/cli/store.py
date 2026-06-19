"""The `store` namespace: universal data-plane operations.

The SAME verbs work everywhere — your laptop, the subprocess runner's container,
an ECS node — because the store is resolved by escalation (env CASCADE_STORE_CONF
first, then a deployment file, then a local file root). So `cascade store fetch`
inside a node uses the engine-injected env config, and the same command on your
laptop uses ./deployment.yaml (or --store) to poke at the same data.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from .utils import store_resolve


def cmd_fetch(args) -> int:
    store = store_resolve(args)
    data = store.get(args.key)
    dest = Path(args.to)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(data)
    return 0


def cmd_stage(args) -> int:
    store = store_resolve(args)
    store.put(args.key, Path(args.from_).read_bytes())
    print(args.key)
    return 0


def cmd_stage_dir(args) -> int:
    """Upload every file under a directory concurrently, keyed by
    ``<prefix>/<relpath>``. Prints the staged keys (one per line)."""
    store = store_resolve(args)
    base = Path(args.from_)
    files = [p for p in base.rglob("*") if p.is_file()]
    prefix = args.prefix.rstrip("/")
    if not files:
        return 0

    def upload(p: Path) -> str:
        rel = p.relative_to(base).as_posix()
        key = f"{prefix}/{rel}" if prefix else rel
        store.put(key, p.read_bytes())
        return key

    keys: list[str] = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(upload, p) for p in files]
        for fut in as_completed(futures):
            keys.append(fut.result())   # raises if any upload failed
    for k in sorted(keys):
        print(k)
    return 0


def _add_store_args(p):
    # store-resolution escalation inputs (env wins over these when present)
    p.add_argument("--runner-config", default=None,
                   help="deployment config YAML (used if CASCADE_STORE_CONF is unset)")
    p.add_argument("--store", default=None, help="local file-store root override")


def add_subcommands(sub):
    s = sub.add_parser("store", help="data-plane store operations (work anywhere)")
    ssub = s.add_subparsers(dest="store_cmd", required=True)

    pf = ssub.add_parser("fetch", help="fetch a store key to a local file")
    pf.add_argument("--key", required=True)
    pf.add_argument("--to", required=True)
    _add_store_args(pf)
    pf.set_defaults(func=cmd_fetch)

    ps = ssub.add_parser("stage", help="upload a local file to a store key")
    ps.add_argument("--from", dest="from_", required=True)
    ps.add_argument("--key", required=True)
    _add_store_args(ps)
    ps.set_defaults(func=cmd_stage)

    psd = ssub.add_parser("stage-dir", help="upload a directory concurrently")
    psd.add_argument("--from", dest="from_", required=True)
    psd.add_argument("--prefix", required=True)
    psd.add_argument("--workers", type=int, default=16)
    _add_store_args(psd)
    psd.set_defaults(func=cmd_stage_dir)
