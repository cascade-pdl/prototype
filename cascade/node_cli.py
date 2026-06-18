"""Node-side CLI utilities — run *inside* the container by the entrypoint.

These are the commands the entrypoint shells out to, so the image's own tool
stays cascade-agnostic (pure local-file in/out). They build the store from
``CASCADE_STORE_CONF`` (the narrow store-conf blob the engine passes down) and
move bytes between the store and the container's local filesystem.

Phases (the three-phase entrypoint shape):
    cascade hook-before     # fetch declared inputs to local files + sanity checks
    <the image's own tool>  # reads local inputs, writes local outputs
    cascade hook-after      # stage outputs + write manifest + sanity checks

Lower-level primitives the hooks build on (also usable directly):
    cascade fetch     --key K --to PATH
    cascade stage     --from PATH --key K
    cascade stage-dir --from DIR --prefix P [--workers N]   (concurrent upload)

Concurrency: stage-dir uploads many files via a bounded thread pool over the
sync store.put — the uploads are I/O-bound (waiting on S3), so threads overlap
the round-trips even on a fraction of a CPU. The Store stays sync; the
concurrency lives here, at the batch.
"""

from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


def _store_from_env():
    """Build the Store from CASCADE_STORE_CONF — the blob the engine passes down
    for every store kind (the subprocess runner rigs a local FileStore's root to
    the container mount, so this is uniform regardless of store or runner)."""
    from .store_config import StoreConf, build_store
    blob = os.environ.get("CASCADE_STORE_CONF")
    if not blob:
        raise RuntimeError(
            "CASCADE_STORE_CONF is not set — the engine passes it for every run; "
            "if you're invoking this utility by hand, set it to the store config JSON")
    return build_store(StoreConf.from_json(blob))


# --------------------------------------------------------------------------- #
# primitives
# --------------------------------------------------------------------------- #
def cmd_fetch(args) -> int:
    store = _store_from_env()
    data = store.get(args.key)
    dest = Path(args.to)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(data)
    return 0


def cmd_stage(args) -> int:
    store = _store_from_env()
    data = Path(args.from_).read_bytes()
    store.put(args.key, data)
    print(args.key)
    return 0


def cmd_stage_dir(args) -> int:
    """Upload every file under a directory concurrently, keyed by
    ``<prefix>/<relpath>``. Prints the staged keys (one per line)."""
    store = _store_from_env()
    base = Path(args.from_)
    files = [p for p in base.rglob("*") if p.is_file()]
    prefix = args.prefix.rstrip("/")

    def upload(p: Path) -> str:
        rel = p.relative_to(base).as_posix()
        key = f"{prefix}/{rel}" if prefix else rel
        store.put(key, p.read_bytes())
        return key

    keys: list[str] = []
    if not files:
        return 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(upload, p) for p in files]
        for fut in as_completed(futures):
            keys.append(fut.result())   # raises if any upload failed
    for k in sorted(keys):
        print(k)
    return 0


# --------------------------------------------------------------------------- #
# hooks (the phase wrappers) — thin to start: fetch + checks / checks + stage
# --------------------------------------------------------------------------- #
def cmd_hook_before(args) -> int:
    """Pre-execution phase: fetch each declared input to a local file and do
    basic sanity checks. Inputs and their local destinations come from the
    engine's env contract (CASCADE_INPUT_KEYS, CASCADE_PORTS)."""
    store = _store_from_env()
    input_keys = json.loads(os.environ.get("CASCADE_INPUT_KEYS", "{}"))
    local_dir = Path(os.environ.get("CASCADE_LOCAL_INPUTS_DIR", "./_cascade_inputs"))
    local_dir.mkdir(parents=True, exist_ok=True)

    staged = {}
    for binding, key in input_keys.items():
        if not key:
            # a gather/empty input may be a JSON array of keys rather than a key;
            # pass it through as-is for the tool to interpret
            continue
        dest = local_dir / f"{binding}.json"
        try:
            dest.write_bytes(store.get(key))
        except Exception as e:
            print(f"[hook-before] FAILED to fetch input '{binding}' (key={key}): {e}")
            return 1
        if dest.stat().st_size == 0:
            print(f"[hook-before] WARNING: input '{binding}' is empty")
        staged[binding] = str(dest)

    # record where inputs landed so the tool / hook-after can find them
    (local_dir / "_staged.json").write_text(json.dumps(staged))
    print(f"[hook-before] fetched {len(staged)} input(s) to {local_dir}")
    return 0


def cmd_hook_after(args) -> int:
    """Post-execution phase: stage the tool's outputs and write the manifest.

    The tool is expected to have written its primary output and any per-item
    outputs into the local output directory. We stage them and emit a manifest
    with the output key + item keys (so a downstream scatter can fan out)."""
    store = _store_from_env()
    output_prefix = os.environ["CASCADE_OUTPUT_PREFIX"]
    manifest_key = os.environ["CASCADE_MANIFEST_KEY"]
    out_dir = Path(os.environ.get("CASCADE_LOCAL_OUTPUT_DIR", "./_cascade_output"))

    output_file = out_dir / "output.json"
    if not output_file.exists():
        print(f"[hook-after] FAILED: tool produced no output.json in {out_dir}")
        return 1

    # stage the primary output
    output_key = f"{output_prefix}/output.json"
    store.put(output_key, output_file.read_bytes())

    # stage any per-item files (out_dir/items/*) concurrently, collect their keys
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

    # stage any blobs the tool produced (out_dir/blobs/*) concurrently
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

    # the tool may have written its own manifest with extra metadata
    # (output_cardinality, custom fields); merge item_keys we discovered.
    manifest = {"output_key": output_key, "item_keys": sorted(item_keys),
                "output_cardinality": len(item_keys)}
    tool_manifest = out_dir / "manifest.json"
    if tool_manifest.exists():
        try:
            extra = json.loads(tool_manifest.read_text())
            manifest = {**manifest, **extra,
                        "output_key": output_key,            # ours wins for these
                        "item_keys": sorted(item_keys) or extra.get("item_keys", [])}
        except Exception:
            pass

    store.put_json(manifest_key, manifest)
    print(f"[hook-after] staged output + {len(item_keys)} item(s)")
    return 0


def add_node_subcommands(sub):
    """Register the node-side subcommands on the main CLI's subparser."""
    pf = sub.add_parser("fetch", help="[node] fetch a store key to a local file")
    pf.add_argument("--key", required=True)
    pf.add_argument("--to", required=True)
    pf.set_defaults(func=cmd_fetch)

    ps = sub.add_parser("stage", help="[node] upload a local file to a store key")
    ps.add_argument("--from", dest="from_", required=True)
    ps.add_argument("--key", required=True)
    ps.set_defaults(func=cmd_stage)

    psd = sub.add_parser("stage-dir", help="[node] upload a directory concurrently")
    psd.add_argument("--from", dest="from_", required=True)
    psd.add_argument("--prefix", required=True)
    psd.add_argument("--workers", type=int, default=16)
    psd.set_defaults(func=cmd_stage_dir)

    hb = sub.add_parser("hook-before", help="[node] pre-exec: fetch inputs + checks")
    hb.set_defaults(func=cmd_hook_before)

    ha = sub.add_parser("hook-after", help="[node] post-exec: stage outputs + manifest")
    ha.add_argument("--workers", type=int, default=16)
    ha.set_defaults(func=cmd_hook_after)
