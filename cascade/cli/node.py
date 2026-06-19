"""The `node` namespace: node-lifecycle operations that only make sense INSIDE a
node's container — the three-phase entrypoint hooks (before/after) and an env
contract check (validate-env). These REQUIRE the full node env contract (output
prefix, manifest key, input keys, ...), unlike the universal `store` commands.

A typical container entrypoint:
    cascade node validate-env        # fail fast if the contract is incomplete
    cascade node before              # fetch inputs -> ./_cascade_inputs
    <run the tool, writing ./_cascade_output/{output.json, items/*, blobs/*}>
    cascade node after               # stage outputs + write the manifest
"""

from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from .utils import store_resolve, assert_node_env


def cmd_validate_env(args) -> int:
    """Assert the node env contract is fully present. A good first line in any
    entrypoint — fails fast and clearly instead of cryptically mid-run."""
    missing = assert_node_env()
    if missing:
        print("[node] env contract incomplete; missing/empty:")
        for k in missing:
            print(f"  - {k}")
        return 1
    print("[node] env contract ok")
    return 0


def cmd_before(args) -> int:
    """Pre-execution: fetch each declared input to a local file + sanity checks."""
    missing = assert_node_env()
    if missing:
        print(f"[node before] env contract incomplete: {', '.join(missing)}")
        return 1
    store = store_resolve(args)
    input_keys = json.loads(os.environ.get("CASCADE_INPUT_KEYS", "{}"))
    local_dir = Path(os.environ.get("CASCADE_LOCAL_INPUTS_DIR", "./_cascade_inputs"))
    local_dir.mkdir(parents=True, exist_ok=True)

    staged = {}
    for binding, key in input_keys.items():
        if not key:
            continue   # gather/empty input — passed through for the tool to interpret
        dest = local_dir / f"{binding}.json"
        try:
            dest.write_bytes(store.get(key))
        except Exception as e:
            print(f"[node before] FAILED to fetch input '{binding}' (key={key}): {e}")
            return 1
        if dest.stat().st_size == 0:
            print(f"[node before] WARNING: input '{binding}' is empty")
        staged[binding] = str(dest)

    (local_dir / "_staged.json").write_text(json.dumps(staged))
    print(f"[node before] fetched {len(staged)} input(s) to {local_dir}")
    return 0


def cmd_after(args) -> int:
    """Post-execution: stage the tool's outputs and write the manifest."""
    missing = assert_node_env()
    if missing:
        print(f"[node after] env contract incomplete: {', '.join(missing)}")
        return 1
    store = store_resolve(args)
    output_prefix = os.environ["CASCADE_OUTPUT_PREFIX"]
    manifest_key = os.environ["CASCADE_MANIFEST_KEY"]
    out_dir = Path(os.environ.get("CASCADE_LOCAL_OUTPUT_DIR", "./_cascade_output"))

    output_file = out_dir / "output.json"
    if not output_file.exists():
        print(f"[node after] FAILED: tool produced no output.json in {out_dir}")
        return 1

    output_key = f"{output_prefix}/output.json"
    store.put(output_key, output_file.read_bytes())

    item_keys: list[str] = []
    items_dir = out_dir / "items"
    if items_dir.is_dir():
        files = [p for p in items_dir.iterdir() if p.is_file()]

        def up(p: Path) -> str:
            key = f"{output_prefix}/items/{p.name}"
            store.put(key, p.read_bytes())
            return key

        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = [pool.submit(up, p) for p in files]
            for fut in as_completed(futures):
                item_keys.append(fut.result())

    blobs_dir = out_dir / "blobs"
    if blobs_dir.is_dir():
        bfiles = [p for p in blobs_dir.rglob("*") if p.is_file()]

        def upb(p: Path) -> str:
            rel = p.relative_to(blobs_dir).as_posix()
            key = f"{output_prefix}/blobs/{rel}"
            store.put(key, p.read_bytes())
            return key

        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            list(as_completed([pool.submit(upb, p) for p in bfiles]))

    manifest = {"output_key": output_key, "item_keys": sorted(item_keys),
                "output_cardinality": len(item_keys)}
    tool_manifest = out_dir / "manifest.json"
    if tool_manifest.exists():
        try:
            extra = json.loads(tool_manifest.read_text())
            manifest = {**manifest, **extra, "output_key": output_key,
                        "item_keys": sorted(item_keys) or extra.get("item_keys", [])}
        except Exception:
            pass

    store.put_json(manifest_key, manifest)
    print(f"[node after] staged output + {len(item_keys)} item(s)")
    return 0


def _add_store_args(p):
    p.add_argument("--runner-config", default=None,
                   help="deployment config YAML (used if CASCADE_STORE_CONF is unset)")
    p.add_argument("--store", default=None, help="local file-store root override")
    p.add_argument("--project-file", default="cascade.toml")


def add_subcommands(sub):
    n = sub.add_parser("node", help="in-container node-lifecycle operations")
    nsub = n.add_subparsers(dest="node_cmd", required=True)

    ve = nsub.add_parser("validate-env", help="check the node env contract is complete")
    ve.set_defaults(func=cmd_validate_env)

    b = nsub.add_parser("before", help="pre-exec: fetch inputs + checks")
    _add_store_args(b)
    b.set_defaults(func=cmd_before)

    a = nsub.add_parser("after", help="post-exec: stage outputs + manifest")
    a.add_argument("--workers", type=int, default=16)
    _add_store_args(a)
    a.set_defaults(func=cmd_after)
