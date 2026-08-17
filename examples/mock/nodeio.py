"""Node-side helper: what a ref needs to talk to Cascade.

**This does not exist in the library yet, and it should.** Every subprocess or
container ref has to do exactly this — parse the ``CASCADE_*`` env, rebuild its two
stores, look up its input bindings, write its outputs — so hand-rolling it per ref is
the wrong shape. It belongs in the library as something like ``cascade/node.py``,
paired with the executor. Kept here for now so the demo does not smuggle in a design
decision.

The contract it implements, from ``cascade.engine.run_spec.to_env``:

- ``CASCADE_STORE_IN``  — dag-scoped store; sibling node outputs are visible.
- ``CASCADE_STORE_OUT`` — instance-scoped store; write bare port names here.
- ``CASCADE_INPUTS``    — per-port ``(scope, key, encoding)``.
- ``CASCADE_ARGS``      — static kwargs from the dag node.
- ``CASCADE_RUN_SPEC``  — identity: run, node, instance.

Note how little a ref has to know: it never computes a path, and it cannot write
outside its own slot, because the writer store is already scoped to it.
"""
from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _ensure_cascade_importable() -> None:
    """Put the repo root on ``sys.path`` if ``cascade`` is not already importable.

    A subprocess ref inherits whatever interpreter ``cmd`` names — and a bare ``python``
    is rarely the one the framework is installed into (a Poetry venv, a uv shim, the
    system Python). Rather than demand a particular interpreter, note that the node-side
    imports need **no third-party packages** (boto3 is imported lazily, yaml is not in the
    chain), so adding the repo root is enough to make the demo run anywhere.

    This is demo scaffolding. A real ref is a container image with cascade installed, and
    the fact that a *subprocess* ref has an environment dependency nothing declares is a
    genuine gap in the ref model, not something to paper over in the library.
    """
    try:
        import cascade  # noqa: F401
        return
    except ModuleNotFoundError:
        pass
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").is_file():
            sys.path.insert(0, str(parent))
            return


_ensure_cascade_importable()

from cascade.engine.binding import InputBindings  # noqa: E402
from cascade.store.registry import decode as decode_store  # noqa: E402


@dataclass
class NodeContext:
    name: str
    run_id: str
    instance_id: str | None
    inputs: InputBindings
    args: dict[str, Any]
    store_in: Any = None
    store_out: Any = None

    def read(self, port: str) -> Any:
        binding = self.inputs.input_for(port)
        if binding is None:
            raise KeyError(f"no input bound for port {port!r}")
        if self.store_in is None:
            raise RuntimeError("no reader store: CASCADE_STORE_IN was not set")
        # read_json, not get_json: resolves a collection descriptor transparently, so a
        # ref never learns whether its input was materialised or left in place
        value = self.store_in.read_json(binding.key, at=binding.scope)

        # the binding carries the declared depth, so a misread fails here rather than
        # somewhere downstream with a confusing symptom
        if binding.depth == 0 and isinstance(value, list):
            raise ValueError(
                f"port {port!r} is declared scalar but received a list of "
                f"{len(value)} — the pipeline and the stored data disagree"
            )
        if binding.depth > 0 and not isinstance(value, list):
            raise ValueError(
                f"port {port!r} is declared {binding.depth}-dimensional but received "
                f"{type(value).__name__}"
            )
        return value

    def has(self, port: str) -> bool:
        return self.inputs.input_for(port) is not None

    def write(self, port: str, value: Any) -> None:
        if self.store_out is None:
            raise RuntimeError("no writer store: CASCADE_STORE_OUT was not set")
        self.store_out.put_json(port, value)

    def log(self, message: str) -> None:
        print(f"[{self.instance_id or self.name}] {message}", flush=True)


def context() -> NodeContext:
    spec = json.loads(os.environ["CASCADE_RUN_SPEC"])
    reader = os.environ.get("CASCADE_STORE_IN")
    writer = os.environ.get("CASCADE_STORE_OUT")
    return NodeContext(
        name=spec["name"],
        run_id=spec["run_id"],
        instance_id=spec.get("instance_id"),
        inputs=InputBindings.decode(json.loads(os.environ.get("CASCADE_INPUTS", "[]"))),
        args=json.loads(os.environ.get("CASCADE_ARGS", "{}")),
        store_in=decode_store(json.loads(reader)) if reader else None,
        store_out=decode_store(json.loads(writer)) if writer else None,
    )