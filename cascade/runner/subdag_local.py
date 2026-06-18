"""In-process (local) runners: builtins for lightweight data-plane reshaping.

These run in the engine's event loop — no container. For pure reshaping over
store references (the collector: concatenate scattered items to one rooted
array; gather/flatten/filter). MUST stay lightweight — heavy compute belongs in
a container or it stalls the loop. This module is also the home for the future
in-process DagRunner (a subdag coordinator as a first-class Runner).
"""

from __future__ import annotations

import asyncio

from .base import Runner, Handle, TaskStatus, RunSpec, RunnerError, _TaskHandle


class BuiltinRunner(Runner):
    """Runs a *builtin* node entirely in-process — no container. For lightweight
    data-plane reshaping that's pure computation over store references: the
    collector (concatenate scattered items to one rooted array), gather/flatten,
    filter, rekey. These are cheap and have nothing to distribute, so a container
    would be pure overhead.

    A builtin node names a registered builtin function via its image field
    (``builtin:collect``); the function receives (store, spec) and writes the
    node's output + manifest to the store, exactly as a container entrypoint
    would. Builtins run in the engine's event loop, so they MUST be lightweight
    (no heavy compute — that belongs in a container, or it stalls the loop).
    """

    def __init__(self, store):
        self.store = store

    def spawn(self, spec: RunSpec) -> Handle:
        async def _go() -> int:
            name = spec.image.split(":", 1)[1] if ":" in spec.image else spec.image
            fn = _BUILTINS.get(name)
            if fn is None:
                raise RunnerError(f"unknown builtin '{name}'; registered: {sorted(_BUILTINS)}")
            fn(self.store, spec)
            return 0
        return _TaskHandle(asyncio.create_task(_go()))




_BUILTINS: dict = {}


def builtin(name: str):
    """Decorator to register a builtin node function."""
    def deco(fn):
        _BUILTINS[name] = fn
        return fn
    return deco


@builtin("collect")
def _collect(store, spec: RunSpec) -> None:
    """Concatenate the items of a gathered input into one rooted array and write
    it as this node's output. The gathered input arrives (per the engine's
    gather) as a JSON array of item keys in CASCADE_INPUT_KEYS under the sole
    binding; we read each and concat into output.json at the node's root."""
    import json as _json
    env = spec.env
    input_keys = _json.loads(env["CASCADE_INPUT_KEYS"])
    out_prefix = env["CASCADE_OUTPUT_PREFIX"]
    manifest_key = env["CASCADE_MANIFEST_KEY"]
    # the single gathered binding holds a JSON array of keys (engine gather form)
    binding, val = next(iter(input_keys.items()))
    item_keys = _json.loads(val) if isinstance(val, str) and val.strip().startswith("[") else val
    items = []
    for k in item_keys:
        v = store.get_json(k)
        if isinstance(v, list):
            items.extend(v)
        else:
            items.append(v)
    out_key = f"{out_prefix}/output.json"
    store.put_json(out_key, items)
    store.put_json(manifest_key, {"output_key": out_key, "output_cardinality": len(items)})

