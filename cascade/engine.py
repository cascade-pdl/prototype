"""The coordinator.

Walks the execution plan wave by wave. For each node it builds a
:class:`~cascade.runner.RunSpec` with the *pointers* the node needs (input keys
resolved from upstream outputs, an output prefix, a manifest key), runs it via
the runner, then reads the node's reported metadata blob from the store to learn
its output key and — crucially for scatter — its output cardinality and item
keys.

Scatter is handled here, at runtime: when a node declares ``scatter``, the
engine reads the upstream node's reported ``item_keys`` and launches one
instance per item, each pointed at its own item. The plan only marked the
scatter *point*; the engine supplies the *count* from the data the upstream
node reported.

The run state (the per-node, per-instance records) is both the execution record
and the result catalog — it maps (run_id, node_id, instance) to the output key
in the store.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field, asdict

from .model import Pipeline
from .plan import ExecutionPlan, PlanNode
from .runner import Runner, RunSpec
from .store import Store


class EngineError(Exception):
    pass


@dataclass
class InstanceRecord:
    instance_key: str
    status: str                       # complete | failed
    output_key: str | None = None
    output_cardinality: int | None = None
    item_keys: list[str] = field(default_factory=list)
    exit_code: int | None = None
    started_at: float | None = None
    completed_at: float | None = None


@dataclass
class NodeRecord:
    node_id: str
    ref_name: str
    status: str = "pending"           # pending | running | complete | failed
    scattered: bool = False
    instances: list[InstanceRecord] = field(default_factory=list)

    def primary_output_key(self) -> str | None:
        """For a non-scattered node, its single output key."""
        if self.instances and self.instances[0].output_key:
            return self.instances[0].output_key
        return None


@dataclass
class RunState:
    run_id: str
    status: str = "running"           # running | complete | failed
    nodes: dict[str, NodeRecord] = field(default_factory=dict)
    inputs: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "inputs": self.inputs,
            "nodes": {
                nid: {
                    "node_id": n.node_id,
                    "ref_name": n.ref_name,
                    "status": n.status,
                    "scattered": n.scattered,
                    "instances": [asdict(i) for i in n.instances],
                }
                for nid, n in self.nodes.items()
            },
        }


class Engine:
    def __init__(self, pipeline: Pipeline, store: Store, runner: Runner):
        self.pipeline = pipeline
        self.store = store
        self.runner = runner

    def run(self, plan: ExecutionPlan, inputs: dict[str, str], run_id: str | None = None) -> RunState:
        """Execute the plan. ``inputs`` maps pipeline input field -> store key
        of the staged input payload (the caller stages root inputs to the store
        before calling, the same way every node's input lives in the store)."""
        run_id = run_id or f"run-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
        state = RunState(run_id=run_id, inputs=inputs)

        for wave in plan.waves:
            for node_id in wave:
                node = plan.nodes[node_id]
                record = self._run_node(plan, node, state, run_id)
                state.nodes[node_id] = record
                if record.status == "failed":
                    state.status = "failed"
                    self._persist(state)
                    raise EngineError(f"node '{node_id}' failed; run {run_id} aborted")

        state.status = "complete"
        self._persist(state)
        return state

    # ------------------------------------------------------------------ #
    def _run_node(self, plan: ExecutionPlan, node: PlanNode, state: RunState, run_id: str) -> NodeRecord:
        ref = self.pipeline.find_ref(node.ref_name)
        if ref is None:
            raise EngineError(f"node '{node.id}' references unknown ref '{node.ref_name}'")

        record = NodeRecord(node_id=node.id, ref_name=node.ref_name, status="running")

        if node.scatter:
            # Explicit scatter: fan out over an upstream collection field's items.
            record.scattered = True
            item_keys = self._resolve_scatter_items(plan, node, state)
            for item_key in item_keys:
                inst = self._run_instance(node, ref, state, run_id,
                                          instance_key=_key_id(item_key),
                                          scatter_item_key=item_key,
                                          carry_instance=None)
                record.instances.append(inst)
                if inst.status == "failed":
                    record.status = "failed"
                    return record
            record.status = "complete"
            return record

        # Scatter carry-through: if this node depends (single mode) on an
        # already-scattered upstream, run once per upstream instance. The
        # scatter propagates downstream until a gather edge collapses it.
        carry = self._carry_instances(node, state)
        if carry is not None:
            record.scattered = True
            for up_inst_key in carry:
                inst = self._run_instance(node, ref, state, run_id,
                                          instance_key=up_inst_key,
                                          scatter_item_key=None,
                                          carry_instance=up_inst_key)
                record.instances.append(inst)
                if inst.status == "failed":
                    record.status = "failed"
                    return record
            record.status = "complete"
            return record

        # Plain single node.
        inst = self._run_instance(node, ref, state, run_id, instance_key="0",
                                  scatter_item_key=None, carry_instance=None)
        record.instances.append(inst)
        record.status = inst.status
        return record

    def _carry_instances(self, node: PlanNode, state: RunState) -> list[str] | None:
        """If this node has a single-mode dependency on a scattered upstream,
        return that upstream's instance keys (so we fan out to match). Returns
        None if no carry-through applies. A gather edge does NOT carry through
        (it collapses the fan-out)."""
        for edge in node.depends_on:
            if edge.is_input or edge.mode == "gather":
                continue
            up = state.nodes.get(edge.node)
            if up is not None and up.scattered and len(up.instances) > 1:
                return [i.instance_key for i in up.instances]
        return None

    def _build_port_plan(self, node: PlanNode, ref, input_keys: dict[str, str]) -> dict:
        """Associate each input binding with its port's local encoding + field
        mapping, and resolve the single output port's encoding + mapping. Used
        by the hooked runner to translate canonical store data <-> the
        container's local format and names."""
        inputs = {}
        for binding, key in input_keys.items():
            port = self._match_port(ref.input, binding)
            inputs[binding] = {
                "key": key,
                "encoding": ref.port_encoding(port) if port else ref.encoding,
                "mapping": port.mapping if port else {},
            }
        # output: single declared output port (the common case)
        out_port = ref.output[0] if ref.output else None
        output = {
            "encoding": ref.port_encoding(out_port) if out_port else ref.encoding,
            "mapping": out_port.mapping if out_port else {},
        }
        return {"inputs": inputs, "output": output}

    @staticmethod
    def _match_port(ports, binding: str):
        """Find the input port matching an edge binding (e.g. '--moth-crop' or
        a field name). Mirrors validate._resolve_input_port."""
        if not ports:
            return None
        cand = binding.lstrip("-")
        for p in ports:
            if p.name == cand or p.name == cand.replace("-", "_") or p.name == binding:
                return p
        return None

    def _run_instance(self, node: PlanNode, ref, state: RunState, run_id: str,
                      instance_key: str, scatter_item_key: str | None,
                      carry_instance: str | None) -> InstanceRecord:
        inst = InstanceRecord(instance_key=instance_key, status="running", started_at=time.time())

        input_keys = self._resolve_input_keys(node, state, scatter_item_key, carry_instance)
        output_prefix = f"runs/{run_id}/{node.id}/{instance_key}"
        manifest_key = f"{output_prefix}/_manifest.json"

        # Build the port plan the translation hooks need: for each input binding,
        # the store key plus the matching port's local encoding + field mapping;
        # and the output port's encoding + mapping. The data plane is canonical
        # JSON; these tell the hooks how to translate to/from the container's
        # local format and names.
        ports = self._build_port_plan(node, ref, input_keys)

        env = {
            "CASCADE_RUN_ID": run_id,
            "CASCADE_NODE_ID": node.id,
            "CASCADE_INSTANCE_KEY": instance_key,
            "CASCADE_INPUT_KEYS": json.dumps(input_keys),
            "CASCADE_OUTPUT_PREFIX": output_prefix,
            "CASCADE_MANIFEST_KEY": manifest_key,
            "CASCADE_ARGS": json.dumps(node.args),
            "CASCADE_PORTS": json.dumps(ports),
        }
        spec = RunSpec(
            run_id=run_id, node_id=node.id, instance_key=instance_key,
            image=ref.image, env=env,
        )

        exit_code = self.runner.run(spec)
        inst.exit_code = exit_code
        inst.completed_at = time.time()

        if exit_code != 0:
            inst.status = "failed"
            return inst

        # read the node-reported metadata blob (control plane reads metadata,
        # never the payload itself)
        meta = self._read_manifest(manifest_key, default_output_prefix=output_prefix)
        inst.output_key = meta.get("output_key") or f"{output_prefix}/output.json"
        inst.output_cardinality = meta.get("output_cardinality")
        inst.item_keys = meta.get("item_keys", [])
        inst.status = "complete"
        return inst

    # ------------------------------------------------------------------ #
    def _resolve_input_keys(self, node: PlanNode, state: RunState,
                            scatter_item_key: str | None, carry_instance: str | None) -> dict[str, str]:
        """Map each edge's binding name -> the store key it should read.

        - $input edges read the staged input (or its default).
        - The scatter field edge (explicit scatter) reads this instance's item.
        - Under carry-through, a single-mode edge on the carried upstream reads
          that upstream's matching instance output.
        - A gather edge reads a JSON list of all upstream instance outputs.
        """
        keys: dict[str, str] = {}
        for edge in node.depends_on:
            binding = (edge.as_ or edge.node).lstrip("-")
            if edge.is_input:
                src_key = state.inputs.get(edge.field)
                if src_key is None:
                    decl = self.pipeline.find_input(edge.field)
                    if decl is not None and decl.default is not None:
                        default_key = f"runs/{state.run_id}/_inputs/{edge.field}.json"
                        if not self.store.has(default_key):
                            self.store.put_json(default_key, decl.default)
                        src_key = default_key
                if src_key is None:
                    raise EngineError(f"node '{node.id}': no staged input for $input.{edge.field}")
                keys[binding] = src_key
                continue

            up = state.nodes.get(edge.node)
            if up is None:
                raise EngineError(f"node '{node.id}': upstream '{edge.node}' has no record")

            # explicit scatter: this edge feeds the per-item key
            if node.scatter and edge.field == node.scatter and scatter_item_key is not None:
                keys[binding] = scatter_item_key
            elif edge.mode == "gather":
                # gather collapses fan-out: list of all upstream instance outputs
                keys[binding] = json.dumps([i.output_key for i in up.instances if i.output_key])
            elif carry_instance is not None:
                # carry-through: read this upstream's matching instance output
                match = next((i for i in up.instances if i.instance_key == carry_instance), None)
                if match is None:
                    # upstream wasn't scattered the same way; fall back to primary
                    keys[binding] = up.primary_output_key() or ""
                else:
                    keys[binding] = match.output_key or ""
            else:
                keys[binding] = up.primary_output_key() or ""
        return keys

    def _resolve_scatter_items(self, plan: ExecutionPlan, node: PlanNode, state: RunState) -> list[str]:
        """Find the upstream node feeding the scatter field and return its
        reported item keys (the runtime cardinality)."""
        for edge in node.depends_on:
            if edge.field == node.scatter and not edge.is_input:
                up = state.nodes.get(edge.node)
                if up is None:
                    raise EngineError(f"scatter on '{node.id}': upstream '{edge.node}' missing")
                # the upstream node must have reported item_keys for its output
                items = up.instances[0].item_keys if up.instances else []
                if not items:
                    raise EngineError(
                        f"scatter on '{node.id}' over '{node.scatter}': upstream "
                        f"'{edge.node}' reported no item_keys in its manifest"
                    )
                return items
        raise EngineError(
            f"scatter on '{node.id}' over field '{node.scatter}' has no matching "
            f"upstream edge"
        )

    def _read_manifest(self, manifest_key: str, default_output_prefix: str) -> dict:
        try:
            if self.store.has(manifest_key):
                return self.store.get_json(manifest_key)
        except Exception:
            pass
        # tolerate a node that didn't write a manifest: assume a single output
        return {"output_key": f"{default_output_prefix}/output.json"}

    def _persist(self, state: RunState) -> None:
        self.store.put_json(f"runs/{state.run_id}/_run_state.json", state.to_dict())


def _key_id(item_key: str) -> str:
    """Derive a stable, readable, filesystem-safe instance id from an item key.
    Uses the final path segment without extension (e.g. .../det-0003.json ->
    det-0003), falling back to a sanitised full key."""
    tail = item_key.rsplit("/", 1)[-1]
    if "." in tail:
        tail = tail.rsplit(".", 1)[0]
    return tail or item_key.replace("/", "_").replace(":", "_")
