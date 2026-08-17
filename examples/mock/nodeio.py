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
from dataclasses import dataclass
from typing import Any

from cascade.engine.binding import InputBindings
from cascade.store.registry import decode as decode_store


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
        # read, not get_json: resolves a collection descriptor transparently, so a ref
        # never learns whether its input was materialised or left in place
        return self.store_in.read_json(binding.key, at=binding.scope)

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