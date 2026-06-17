"""The coordinator (hierarchical-key edition).

Walks the execution plan wave by wave. The unit of execution is a node
*instance*, identified by an :class:`~cascade.hkey.InstanceKey` — a path that
grows one segment per scatter level. This makes nested scatter the general case:

  - a node with no scattered inputs and no ``scatter:`` runs once, at the root ()
  - ``scatter: field`` appends a segment per reported item, FOR EACH parent
    instance the node already has (so scattering a node that is itself nested
    fans out within each parent — true nested scatter)
  - a single-mode dependency on a scattered upstream carries that upstream's
    paths through unchanged (carry-through, at any depth)
  - a ``gather`` dependency collapses one level: the node runs at the PARENT of
    the gathered upstream's instances, receiving all siblings' outputs

The data plane / control plane split is unchanged: nodes own payloads (read/write
the store by key); the engine moves only pointers and reads node-reported
metadata. Store keys embed the instance path so lineage is visible on disk.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field, asdict

from .hkey import InstanceKey
from .model import Pipeline
from .plan import ExecutionPlan, PlanNode
from .runner import Runner, RunSpec
from .store import Store


class EngineError(Exception):
    pass


@dataclass
class InstanceRecord:
    instance_key: InstanceKey
    status: str                       # complete | failed
    output_key: str | None = None
    output_media_type: str | None = None
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

    # --- instance lookup helpers (the routing surface) --------------------- #
    def by_key(self, key: InstanceKey) -> "InstanceRecord | None":
        return next((i for i in self.instances if i.instance_key == key), None)

    def instance_keys(self) -> list[InstanceKey]:
        return [i.instance_key for i in self.instances]

    def primary_output_key(self) -> str | None:
        if self.instances and self.instances[0].output_key:
            return self.instances[0].output_key
        return None

    def matching_instance(self, key: InstanceKey) -> "InstanceRecord | None":
        """The upstream instance whose path is an ancestor of (or equal to)
        ``key`` — the one this instance descends from. Used for carry-through /
        nested reads where the consumer is deeper than (or equal to) the
        producer."""
        exact = self.by_key(key)
        if exact is not None:
            return exact
        candidates = [i for i in self.instances if i.instance_key.is_ancestor_of(key)]
        if len(candidates) == 1:
            return candidates[0]
        if len(candidates) > 1:
            return max(candidates, key=lambda i: i.instance_key.depth)
        return None

    def descendants_under(self, ancestor: InstanceKey) -> list["InstanceRecord"]:
        """All instances nested anywhere under ``ancestor`` (at any greater
        depth). Used by gather: a node at depth D gathers every upstream
        instance below its path, collapsing all intervening levels."""
        return [i for i in self.instances
                if i.instance_key.depth > ancestor.depth
                and ancestor.is_ancestor_of(i.instance_key)]

    def children_under(self, parent: InstanceKey) -> list["InstanceRecord"]:
        """All instances whose immediate parent path equals ``parent``."""
        return [i for i in self.instances if i.instance_key.depth > parent.depth
                and i.instance_key.parent() == parent]


@dataclass
class RunState:
    run_id: str
    status: str = "running"
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
                    "instances": [
                        {**{k: v for k, v in asdict(i).items() if k != "instance_key"},
                         "instance_key": i.instance_key.render()}
                        for i in n.instances
                    ],
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
    # Instance-set computation: the heart of the hierarchical model.
    # ------------------------------------------------------------------ #
    def _instance_set(self, node: PlanNode, state: RunState) -> list[InstanceKey]:
        """Determine the set of instance keys this node runs as, from its
        dependencies. Replaces the old three-branch logic with the uniform
        scatter/carry/gather model."""
        base = self._base_instances(node, state)
        if node.scatter:
            return self._apply_scatter(node, state, base)
        return base

    def _base_instances(self, node: PlanNode, state: RunState) -> list[InstanceKey]:
        """The instance set implied by the node's dependencies, before its own
        scatter is applied.

        Two kinds of dependency shape the set:
          - CARRY-THROUGH (single-mode dep on a scattered upstream): the node
            inherits the upstream's instance paths, pinning it to that depth.
          - GATHER: collapses the upstream's instances down to the depth this
            node otherwise sits at. Gather is NOT "minus one level"; it collapses
            to the CONSUMER's base depth — so an otherwise-unscattered consumer
            (no carry-through) gathers everything down to the root, while a
            consumer pinned to depth D by carry-through gathers to depth D.

        We compute carry-through first (it determines the node's depth); gather
        deps are then expressed relative to that depth at read time.

        LIMITATION (partial gather): because gather collapses to the consumer's
        *base depth*, a bare gather on an otherwise-unscattered node collapses
        ALL levels to the root. There is currently no way to say "gather just one
        level, staying nested" (e.g. collect detections *per image* into a
        depth-1 instance per image). To get per-group collection today, keep the
        grouping node carried-through (so it is pinned to the group's depth) and
        have it consume the per-instance outputs — i.e. don't use a bare gather
        for partial collapse. Full "gather to depth N" is a known future
        extension; the common case (nested scatter then gather-to-root, e.g. the
        moth pipeline's final tracker) works today.
        """
        carry_sets: list[list[InstanceKey]] = []
        for edge in node.depends_on:
            if edge.is_input or edge.mode == "gather":
                continue
            up = state.nodes.get(edge.node)
            if up is None or not up.instances:
                continue
            if up.scattered and any(i.instance_key.depth > 0 for i in up.instances):
                carry_sets.append(up.instance_keys())

        if carry_sets:
            carry_sets.sort(key=lambda s: max((k.depth for k in s), default=0), reverse=True)
            return carry_sets[0]

        # no carry-through: the node sits at the root; any gather dep collapses
        # all the way to a single root instance.
        return [InstanceKey()]

    def _apply_scatter(self, node: PlanNode, state: RunState,
                       base: list[InstanceKey]) -> list[InstanceKey]:
        """Append a scatter segment per reported item, for each base instance.
        Items come from the upstream feeding the scatter field, matched to each
        base instance (so nested scatter reads the right parent's items)."""
        up_node, _edge = self._scatter_upstream(node, state)
        result: list[InstanceKey] = []
        for parent in base:
            up_inst = up_node.matching_instance(parent) if up_node else None
            if up_inst is None:
                continue
            for item_id, _item_key in self._items_of(up_inst):
                result.append(parent.child(node.scatter, item_id))
        return result

    def _scatter_upstream(self, node: PlanNode, state: RunState):
        """The (NodeRecord, edge) whose field is this node's scatter axis."""
        for edge in node.depends_on:
            if not edge.is_input and edge.field == node.scatter:
                up = state.nodes.get(edge.node)
                if up is None:
                    raise EngineError(
                        f"scatter on '{node.id}' over '{node.scatter}': upstream "
                        f"'{edge.node}' has no record")
                return up, edge
        raise EngineError(
            f"scatter on '{node.id}' over field '{node.scatter}' has no matching "
            f"upstream edge")

    @staticmethod
    def _items_of(inst: "InstanceRecord") -> list[tuple[str, str]]:
        """The (item_id, item_store_key) pairs an instance reported for scatter."""
        return [(_item_id_from_key(k), k) for k in inst.item_keys]

    # ------------------------------------------------------------------ #
    def _run_node(self, plan: ExecutionPlan, node: PlanNode, state: RunState, run_id: str) -> NodeRecord:
        ref = self.pipeline.find_ref(node.ref_name)
        if ref is None:
            raise EngineError(f"node '{node.id}' references unknown ref '{node.ref_name}'")

        record = NodeRecord(node_id=node.id, ref_name=node.ref_name, status="running")
        instances = self._instance_set(node, state)
        record.scattered = not (len(instances) == 1 and instances[0].depth == 0)

        for ikey in instances:
            inst = self._run_instance(node, ref, state, run_id, ikey)
            record.instances.append(inst)
            if inst.status == "failed":
                record.status = "failed"
                return record
        record.status = "complete"
        return record

    def _run_instance(self, node: PlanNode, ref, state: RunState, run_id: str,
                      ikey: InstanceKey) -> InstanceRecord:
        inst = InstanceRecord(instance_key=ikey, status="running", started_at=time.time())

        input_keys = self._resolve_input_keys(node, state, ikey)
        frag = ikey.as_store_fragment()
        output_prefix = f"runs/{run_id}/{node.id}/{frag}"
        manifest_key = f"{output_prefix}/_manifest.json"
        ports = self._build_port_plan(node, ref, input_keys)

        env = {
            "CASCADE_RUN_ID": run_id,
            "CASCADE_NODE_ID": node.id,
            "CASCADE_INSTANCE_KEY": ikey.render(),
            "CASCADE_INPUT_KEYS": json.dumps(input_keys),
            "CASCADE_OUTPUT_PREFIX": output_prefix,
            "CASCADE_MANIFEST_KEY": manifest_key,
            "CASCADE_ARGS": json.dumps(node.args),
            "CASCADE_PORTS": json.dumps(ports),
        }
        spec = RunSpec(run_id=run_id, node_id=node.id, instance_key=ikey.render(),
                       image=ref.image, env=env)

        exit_code = self.runner.run(spec)
        inst.exit_code = exit_code
        inst.completed_at = time.time()
        if exit_code != 0:
            inst.status = "failed"
            return inst

        meta = self._read_manifest(manifest_key, default_output_prefix=output_prefix)
        out_is_binary = ports.get("output", {}).get("binary", False)
        default_out = f"{output_prefix}/output.blob" if out_is_binary else f"{output_prefix}/output.json"
        inst.output_key = meta.get("output_key") or default_out
        inst.output_media_type = ports.get("output", {}).get("media_type") if out_is_binary else None
        inst.output_cardinality = meta.get("output_cardinality")
        inst.item_keys = meta.get("item_keys", [])
        inst.status = "complete"
        return inst

    # ------------------------------------------------------------------ #
    def _resolve_input_keys(self, node: PlanNode, state: RunState,
                            ikey: InstanceKey) -> dict[str, str]:
        """Map each edge's binding -> the store key it reads, using the instance
        path to find the right upstream instance."""
        keys: dict[str, str] = {}
        for edge in node.depends_on:
            binding = (edge.as_ or edge.node).lstrip("-")

            if edge.is_input:
                keys[binding] = self._staged_input_key(node, state, edge)
                continue

            up = state.nodes.get(edge.node)
            if up is None:
                raise EngineError(f"node '{node.id}': upstream '{edge.node}' has no record")

            # this edge is the scatter axis: feed this instance's own item
            if node.scatter and edge.field == node.scatter and ikey.depth > 0 \
                    and ikey.last_axis() == node.scatter:
                up_inst = up.matching_instance(ikey.parent())
                item_key = self._item_key_for(up_inst, ikey.last_item()) if up_inst else None
                keys[binding] = item_key or ""
                continue

            if edge.mode == "gather":
                members = up.descendants_under(ikey)
                keys[binding] = json.dumps([m.output_key for m in members if m.output_key])
                continue

            # carry-through / nested single read
            match = up.matching_instance(ikey) or (up.instances[0] if up.instances else None)
            keys[binding] = (match.output_key if match else "") or ""
        return keys

    def _staged_input_key(self, node: PlanNode, state: RunState, edge) -> str:
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
        return src_key

    @staticmethod
    def _item_key_for(up_inst: "InstanceRecord | None", item_id: str) -> str | None:
        if up_inst is None:
            return None
        for k in up_inst.item_keys:
            if _item_id_from_key(k) == item_id:
                return k
        return None

    # ------------------------------------------------------------------ #
    def _build_port_plan(self, node: PlanNode, ref, input_keys: dict[str, str]) -> dict:
        from .types import parse_type
        inputs = {}
        for binding, key in input_keys.items():
            port = self._match_port(ref.input, binding)
            is_binary, media = False, None
            if port is not None:
                try:
                    pt = parse_type(port.type)
                    is_binary, media = pt.is_binary, pt.media_type
                except Exception:
                    pass
            inputs[binding] = {
                "key": key,
                "encoding": (ref.port_encoding(port) if port else ref.encoding),
                "mapping": (port.mapping if port else {}),
                "binary": is_binary,
                "media_type": media,
            }
        out_port = ref.output[0] if ref.output else None
        out_binary, out_media = False, None
        if out_port is not None:
            try:
                ot = parse_type(out_port.type)
                out_binary, out_media = ot.is_binary, ot.media_type
            except Exception:
                pass
        output = {
            "encoding": (ref.port_encoding(out_port) if out_port else ref.encoding),
            "mapping": (out_port.mapping if out_port else {}),
            "binary": out_binary,
            "media_type": out_media,
        }
        return {"inputs": inputs, "output": output}

    @staticmethod
    def _match_port(ports, binding: str):
        if not ports:
            return None
        cand = binding.lstrip("-")
        for p in ports:
            if p.name == cand or p.name == cand.replace("-", "_") or p.name == binding:
                return p
        return None

    def _read_manifest(self, manifest_key: str, default_output_prefix: str) -> dict:
        try:
            if self.store.has(manifest_key):
                return self.store.get_json(manifest_key)
        except Exception:
            pass
        return {"output_key": f"{default_output_prefix}/output.json"}

    def _persist(self, state: RunState) -> None:
        self.store.put_json(f"runs/{state.run_id}/_run_state.json", state.to_dict())


def _item_id_from_key(item_key: str) -> str:
    """Derive a stable, readable id from an item's store key (its last path
    segment without extension). e.g. .../items/det-0003.json -> det-0003."""
    tail = item_key.rsplit("/", 1)[-1]
    if "." in tail:
        tail = tail.rsplit(".", 1)[0]
    return tail or item_key.replace("/", "_").replace(":", "_")
