"""Hooked runner: performs the canonical<->local data-plane translation hooks
around an inner runner (the local/dev realisation of the node-side hook
contract). For distributed runs the same translation ships in the image.
"""

from __future__ import annotations

import asyncio

from .base import Runner, Handle, RunSpec, _TaskHandle


class HookedRunner(Runner):
    """Wraps an inner runner and performs the data-plane translation hooks at
    the node boundary:

      input hook  — for each input key, read the canonical (JSON) payload from
                    the store, relabel canonical->local field names, re-encode
                    to the port's local encoding, and stage a *local* copy the
                    container reads from.
      output hook — read the container's local-format output, relabel
                    local->canonical, re-encode to canonical JSON, store it.

    This keeps the inner runner dumb (it just launches the container against the
    already-translated local files) and keeps the store canonical. The
    translation is pure representation change (rename + re-encode), never
    computation.

    The wrapper needs to know each port's encoding and mapping, so it is given
    the resolved ``Ref`` for the node plus the binding->port association the
    engine computed. To stay decoupled, the engine passes the per-instance
    *port plan* via ``spec.env['CASCADE_PORTS']`` (JSON), and the store, so the
    hooks can read/write.

    NOTE: with real containers, this translation belongs *inside* the image's
    entrypoint wrapper (node-side), since only the node should touch payloads.
    HookedRunner is the local/dev realisation of that contract: it performs the
    same steps coordinator-side around a local container run, which is
    acceptable for single-machine runs. For distributed runs, the same hook
    library ships in the image and runs there instead.
    """

    def __init__(self, inner: Runner, store, local_dir: str = "/tmp/cascade_local"):
        from pathlib import Path
        self.inner = inner
        self.store = store
        self.local_dir = Path(local_dir)
        self.local_dir.mkdir(parents=True, exist_ok=True)

    def spawn(self, spec: RunSpec) -> Handle:
        return _TaskHandle(asyncio.create_task(self._run_hooked(spec)))

    async def _run_hooked(self, spec: RunSpec) -> int:
        import json as _json
        from pathlib import Path
        from .. import hooks

        ports = _json.loads(spec.env.get("CASCADE_PORTS", "{}"))
        # ports = {
        #   "inputs":  { binding: {"key": storekey, "encoding": "csv", "mapping": {...}} },
        #   "output":  { "encoding": "csv", "mapping": {...} }
        # }

        # --- input hook: canonical store -> local files the container reads ---
        local_inputs: dict[str, str] = {}
        for binding, p in ports.get("inputs", {}).items():
            canonical = self.store.get(p["key"])
            is_bin = p.get("binary", False)
            local_bytes = hooks.to_container(
                canonical, p.get("encoding", "json"), p.get("mapping") or {}, is_binary=is_bin
            )
            ext = _ext_for(is_bin, p.get("media_type"), p.get("encoding", "json"))
            local_path = self.local_dir / (
                f"{spec.node_id}_{spec.instance_key}_{binding}{ext}".replace("/", "_")
            )
            Path(local_path).write_bytes(local_bytes)
            local_inputs[binding] = str(local_path)

        # tell the inner runner / container where the local input files are and
        # where to write its local output
        out_is_bin = ports.get("output", {}).get("binary", False)
        out_enc = ports.get("output", {}).get("encoding", "json")
        out_media = ports.get("output", {}).get("media_type")
        out_ext = _ext_for(out_is_bin, out_media, out_enc)
        local_out = self.local_dir / (
            f"{spec.node_id}_{spec.instance_key}_out{out_ext}".replace("/", "_")
        )
        inner_env = dict(spec.env)
        inner_env["CASCADE_LOCAL_INPUTS"] = _json.dumps(local_inputs)
        inner_env["CASCADE_LOCAL_OUTPUT"] = str(local_out)

        inner_spec = RunSpec(
            run_id=spec.run_id, node_id=spec.node_id, instance_key=spec.instance_key,
            image=spec.image, env=inner_env,
        )
        code = await self.inner.run(inner_spec)
        if code != 0:
            return code

        # --- output hook: container's local output -> canonical store ---
        out_mapping = ports.get("output", {}).get("mapping") or {}
        output_prefix = spec.env["CASCADE_OUTPUT_PREFIX"]
        # Uniform, predictable store key regardless of kind: structured outputs
        # are canonical JSON at output.json; binary blobs are raw bytes at
        # output.blob, with the media type recorded as metadata (in the manifest),
        # NOT encoded in the key — so retrieval is uniform and the engine always
        # knows where to look.
        canonical_key = (
            f"{output_prefix}/output.blob" if out_is_bin else f"{output_prefix}/output.json"
        )
        if Path(local_out).exists():
            local_bytes = Path(local_out).read_bytes()
            canonical_bytes = hooks.from_container(
                local_bytes, out_enc, out_mapping, is_binary=out_is_bin
            )
            self.store.put(canonical_key, canonical_bytes)
            # record the blob's media type as metadata alongside the output
            if out_is_bin:
                meta_key = f"{output_prefix}/_blob_meta.json"
                self.store.put_json(meta_key, {
                    "media_type": ports.get("output", {}).get("media_type"),
                    "output_key": canonical_key,
                })
        return 0


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _ext_for(is_binary: bool, media_type: str | None, encoding: str) -> str:
    """Pick a local file extension for the staged file. For binary blobs, derive
    it from the media subtype (image/png -> .png); for structured data, from the
    encoding (json/csv)."""
    if is_binary:
        if media_type and "/" in media_type:
            sub = media_type.split("/", 1)[1]
            if sub and sub != "*":
                return f".{sub}"
        return ".bin"
    return f".{encoding}"


# --------------------------------------------------------------------------- #
# Runner registry: resolve a RunnerKind to a (lazily-built, cached) Runner
# instance, configured with the deployment config for this environment.
