"""The runner: launch one node instance as a container.

The runner is deliberately dumb. It receives a :class:`RunSpec` — an image, and
the *pointers* (store keys) the node needs as environment variables — launches
the container, waits, and returns the exit code. It never reads or writes
payloads; the node's own entrypoint does that, using the keys in its env.

The container entrypoint contract (what your hand-built images implement):

    CASCADE_RUN_ID        the run id
    CASCADE_NODE_ID       this node's id
    CASCADE_INSTANCE_KEY  scatter instance key ("0" if not scattered)
    CASCADE_INPUT_KEYS    JSON: { binding_name: store_key, ... }
    CASCADE_OUTPUT_PREFIX store key prefix to write output(s) under
    CASCADE_MANIFEST_KEY  store key to write the result metadata blob to
    CASCADE_ARGS          JSON: the node's static args (kwargs)

The entrypoint reads its inputs from the store at CASCADE_INPUT_KEYS, does its
work, writes its output payload under CASCADE_OUTPUT_PREFIX, and writes a small
metadata blob to CASCADE_MANIFEST_KEY containing at least:
    { "output_key": "...", "output_cardinality": N, "item_keys": [...] }
"""

from __future__ import annotations

import json
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class RunSpec:
    run_id: str
    node_id: str
    instance_key: str
    image: str
    env: dict[str, str] = field(default_factory=dict)


class Runner(ABC):
    @abstractmethod
    def run(self, spec: RunSpec) -> int:
        """Run the node instance to completion; return its exit code."""


class SubprocessRunner(Runner):
    """Runs the node as a local Docker container via ``docker run``.

    The store must be reachable from inside the container. For a local
    :class:`~cascade.store.FileStore`, that means bind-mounting the store root
    into the container at a known path and pointing the entrypoint at it.
    """

    def __init__(self, store_mount: str | None = None, extra_args: list[str] | None = None):
        # store_mount: "host_path:container_path" for a FileStore-backed local run
        self.store_mount = store_mount
        self.extra_args = extra_args or []

    def run(self, spec: RunSpec) -> int:
        cmd = ["docker", "run", "--rm"]
        if self.store_mount:
            cmd += ["-v", self.store_mount]
        for k, v in spec.env.items():
            cmd += ["-e", f"{k}={v}"]
        cmd += self.extra_args
        cmd.append(spec.image)
        proc = subprocess.run(cmd)
        return proc.returncode


class EcsRunner(Runner):
    """Launches the node as an ECS task and polls until it stops. Stubbed —
    wire up boto3 ``run_task`` + ``describe_tasks`` for scaled fan-out.

    Same interface as :class:`SubprocessRunner`; the engine doesn't care which
    backend runs a node, which is what lets the moth pipeline scale to 100
    concurrent tasks without changing anything above the runner.
    """

    def __init__(self, cluster: str, region: str, poll_interval: float = 5.0):
        self.cluster = cluster
        self.region = region
        self.poll_interval = poll_interval

    def run(self, spec: RunSpec) -> int:  # pragma: no cover - stub
        # boto3 ecs.run_task(... containerOverrides=[{environment: spec.env}] ...)
        # then poll describe_tasks until STOPPED, read the container exit code.
        raise NotImplementedError("EcsRunner.run: wire up boto3 run_task/describe_tasks")


class EchoRunner(Runner):
    """A no-op runner for tests/dry-runs: prints what it would launch and
    succeeds. Lets you exercise the engine without Docker."""

    def __init__(self, log: list[str] | None = None):
        self.log = log if log is not None else []

    def run(self, spec: RunSpec) -> int:
        line = (
            f"[echo] would run {spec.image} "
            f"node={spec.node_id} instance={spec.instance_key} "
            f"inputs={spec.env.get('CASCADE_INPUT_KEYS')}"
        )
        self.log.append(line)
        print(line)
        return 0


# --------------------------------------------------------------------------- #
# HookedRunner — runs the node-side translation hooks around an inner runner.
# --------------------------------------------------------------------------- #
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

    def run(self, spec: RunSpec) -> int:
        import json as _json
        from pathlib import Path
        from . import hooks

        ports = _json.loads(spec.env.get("CASCADE_PORTS", "{}"))
        # ports = {
        #   "inputs":  { binding: {"key": storekey, "encoding": "csv", "mapping": {...}} },
        #   "output":  { "encoding": "csv", "mapping": {...} }
        # }

        # --- input hook: canonical store -> local files the container reads ---
        local_inputs: dict[str, str] = {}
        for binding, p in ports.get("inputs", {}).items():
            canonical = self.store.get(p["key"])
            local_bytes = hooks.to_container(canonical, p.get("encoding", "json"), p.get("mapping") or {})
            local_path = self.local_dir / f"{spec.node_id}_{spec.instance_key}_{binding}".replace("/", "_")
            Path(local_path).write_bytes(local_bytes)
            local_inputs[binding] = str(local_path)

        # tell the inner runner / container where the local input files are and
        # where to write its local output
        out_enc = ports.get("output", {}).get("encoding", "json")
        local_out = self.local_dir / f"{spec.node_id}_{spec.instance_key}_out.{out_enc}".replace("/", "_")
        inner_env = dict(spec.env)
        inner_env["CASCADE_LOCAL_INPUTS"] = _json.dumps(local_inputs)
        inner_env["CASCADE_LOCAL_OUTPUT"] = str(local_out)

        inner_spec = RunSpec(
            run_id=spec.run_id, node_id=spec.node_id, instance_key=spec.instance_key,
            image=spec.image, env=inner_env,
        )
        code = self.inner.run(inner_spec)
        if code != 0:
            return code

        # --- output hook: container's local output -> canonical store ---
        out_mapping = ports.get("output", {}).get("mapping") or {}
        output_prefix = spec.env["CASCADE_OUTPUT_PREFIX"]
        canonical_key = f"{output_prefix}/output.json"
        if Path(local_out).exists():
            local_bytes = Path(local_out).read_bytes()
            canonical_bytes = hooks.from_container(local_bytes, out_enc, out_mapping)
            self.store.put(canonical_key, canonical_bytes)
        return 0
