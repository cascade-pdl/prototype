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

import asyncio
import json
import time
import uuid
from dataclasses import dataclass, field, asdict

from .hkey import InstanceKey
from .model import Pipeline
from .plan import ExecutionPlan, PlanNode, build_plan
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
    def __init__(self, pipeline: Pipeline, store: Store, runner=None, *, runners=None,
                 max_concurrency: int = 1, store_conf=None):
        """``runners`` is a RunnerRegistry (resolves per-ref runner kind). For
        backward compatibility, a single ``runner`` may be passed instead and is
        used for every node regardless of kind.

        ``max_concurrency`` bounds how many node *instances* run at once (the
        scatter fan-out). Default 1 = fully sequential (safe for a laptop). Raise
        it for parallel fan-out (e.g. 100 ECS tasks). Runners are natively async,
        so instances multiplex on one thread; the semaphore caps in-flight count.

        ``store_conf`` (a store_config.StoreConf) is serialized into each
        container's ``CASCADE_STORE_CONF`` so the container's fetch/stage
        utilities build the *same* store the engine uses."""
        self.pipeline = pipeline
        self.store = store
        self.runner = runner          # single-runner fallback (tests/demos)
        self.runners = runners        # RunnerRegistry (per-ref dispatch)
        self.max_concurrency = max_concurrency
        self.store_conf = store_conf  # StoreConf passed down to containers

    def _runner_for(self, ref, node=None):
        """Resolve the runner for a node/ref. Builtin nodes use the in-process
        BuiltinRunner; otherwise the registry by ref runner kind, else the single
        fallback runner."""
        if node is not None and getattr(node, "kind", "ref") == "builtin":
            from .runner import BuiltinRunner
            return BuiltinRunner(self.store)
        if self.runners is not None:
            return self.runners.get(ref.runner.kind)
        return self.runner

    def run(self, plan: ExecutionPlan, inputs: dict[str, str], run_id: str | None = None) -> RunState:
        """Synchronous entry point — wraps the async engine. Existing callers
        (CLI, tests) use this unchanged."""
        return asyncio.run(self.run_async(plan, inputs, run_id))

    async def run_async(self, plan: ExecutionPlan, inputs: dict[str, str],
                        run_id: str | None = None) -> RunState:
        run_id = run_id or f"run-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
        state = RunState(run_id=run_id, inputs=inputs)
        # bounds concurrent node *instances* across the whole run. Runners are
        # natively async (they await their waits — docker process, ECS poll
        # loop), so many instances multiplex on one thread; the semaphore caps
        # how many are in flight at once.
        semaphore = asyncio.Semaphore(self.max_concurrency)

        # waves are dependency-ordered → sequential. Nodes within a wave are run
        # sequentially here too (instance-level concurrency is what scales the
        # scatter); cross-node concurrency can be added later.
        for wave in plan.waves:
            for node_id in wave:
                node = plan.nodes[node_id]
                record = await self._run_node(plan, node, state, run_id, semaphore)
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
    async def _run_node(self, plan: ExecutionPlan, node: PlanNode, state: RunState,
                        run_id: str, semaphore: "asyncio.Semaphore") -> NodeRecord:
        kind = getattr(node, "kind", "ref")
        if kind == "dag":
            return await self._run_subdag_node(plan, node, state, run_id, semaphore)

        ref = self.pipeline.find_ref(node.ref_name)
        if ref is None and kind == "builtin":
            # builtin nodes have no ref; synthesize a minimal one carrying the
            # builtin image id (builtin:<name>) and a subprocess runner spec
            # placeholder (the engine routes builtins to the BuiltinRunner).
            ref = self._builtin_ref(node)
        if ref is None:
            raise EngineError(f"node '{node.id}' references unknown ref '{node.ref_name}'")

        record = NodeRecord(node_id=node.id, ref_name=node.ref_name, status="running")
        instances = self._instance_set(node, state)
        record.scattered = not (len(instances) == 1 and instances[0].depth == 0)

        async def run_one(ikey: InstanceKey) -> InstanceRecord:
            async with semaphore:
                return await self._run_instance(node, ref, state, run_id, ikey)

        results = await asyncio.gather(*(run_one(k) for k in instances))
        record.instances = list(results)
        if any(i.status == "failed" for i in record.instances):
            record.status = "failed"
        else:
            record.status = "complete"
        return record

    def _builtin_ref(self, node: PlanNode):
        """Synthesize a Ref for a builtin node (image = builtin:<name>)."""
        from .model import Ref, IoDecl
        return Ref(name=node.id, image=node.ref_name)

    async def _run_subdag_node(self, plan: ExecutionPlan, node: PlanNode,
                               state: RunState, run_id: str,
                               semaphore: "asyncio.Semaphore") -> NodeRecord:
        """In-process dag runner: a subdag node runs its body in its own scope,
        once per instance of the node, under a child instance key. The subdag's
        $input is wired from the node's depends_on (lexical scoping — the subdag
        sees only what the parent passes). Its declared outputs are bound to
        internal nodes and surfaced at the subdag node's instance location, so
        the parent consumes the subdag exactly like a leaf node.
        """
        subdag = self.pipeline.find_dag(node.ref_name)
        if subdag is None:
            raise EngineError(f"subdag node '{node.id}' references unknown dag '{node.ref_name}'")

        record = NodeRecord(node_id=node.id, ref_name=node.ref_name, status="running")
        instances = self._instance_set(node, state)
        record.scattered = not (len(instances) == 1 and instances[0].depth == 0)

        async def run_one(ikey: InstanceKey) -> InstanceRecord:
            async with semaphore:
                return await self._run_subdag_instance(node, subdag, state, run_id, ikey)

        results = await asyncio.gather(*(run_one(k) for k in instances))
        record.instances = list(results)
        record.status = "failed" if any(i.status == "failed" for i in record.instances) else "complete"
        return record

    async def _run_subdag_instance(self, node: PlanNode, subdag, state: RunState,
                                   run_id: str, ikey: InstanceKey) -> InstanceRecord:
        inst = InstanceRecord(instance_key=ikey, status="running", started_at=time.time())

        # wire the parent's inputs into the subdag's $input scope: each of the
        # node's depends_on resolves to a store key, bound by its `as`/field name
        parent_inputs = self._resolve_input_keys(node, state, ikey)

        # build a sub-pipeline whose dag IS the subdag body, whose $input is the
        # wired parent inputs, sharing refs/types with the parent pipeline
        from .model import Pipeline
        sub_pipeline = Pipeline(
            types=self.pipeline.types,
            input=[],  # subdag inputs are provided as pre-staged keys below
            refs=self.pipeline.refs,
            dags=self.pipeline.dags,
            dag=subdag.nodes,
        )
        sub_plan = build_plan(sub_pipeline)

        # run the subdag body in-process under a child run scope. The child run
        # id namespaces the subdag's instance keys under this node+instance, so
        # the global data-plane tree stays coherent.
        child_run_id = f"{run_id}/{node.id}/{ikey.as_store_fragment()}"
        sub_engine = Engine(sub_pipeline, self.store, runner=self.runner,
                            runners=self.runners, max_concurrency=self.max_concurrency,
                            store_conf=self.store_conf)
        try:
            sub_state = await sub_engine.run_async(sub_plan, parent_inputs, run_id=child_run_id)
        except EngineError:
            inst.status = "failed"
            inst.completed_at = time.time()
            return inst

        # bind declared outputs: surface each subdag output (from an internal
        # node, collapsed to the subdag root) at this node's output location
        output_prefix = f"runs/{run_id}/{node.id}/{ikey.as_store_fragment()}"
        bound = self._bind_subdag_outputs(subdag, sub_state, output_prefix)
        inst.output_key = bound.get("output_key")
        inst.output_cardinality = bound.get("output_cardinality")
        inst.item_keys = bound.get("item_keys", [])
        inst.status = "complete"
        inst.completed_at = time.time()
        return inst

    def _bind_subdag_outputs(self, subdag, sub_state: RunState, output_prefix: str) -> dict:
        """Surface the subdag's declared outputs at output_prefix. Each declared
        output binds to an internal node's root-instance output. With a single
        declared output we write it as the node's output.json; multiple outputs
        are written under named keys plus a manifest."""
        if not subdag.outputs:
            # default: bind the sole sink node (no declared outputs) — use the
            # last node's root instance output
            return {}
        bound_items = {}
        primary = None
        for spec in subdag.outputs:
            out_name = spec["name"]
            from_node = spec.get("from") or spec.get("from_node")
            nr = sub_state.nodes.get(from_node)
            if nr is None or not nr.instances:
                continue
            # collapse to subdag root: the root instance (depth 0) of from_node
            root_inst = next((i for i in nr.instances if i.instance_key.depth == 0), nr.instances[0])
            src_key = root_inst.output_key
            data = self.store.get_json(src_key)
            dest = f"{output_prefix}/{out_name}.json"
            self.store.put_json(dest, data)
            bound_items[out_name] = dest
            if primary is None:
                primary = dest
        # primary output.json points at the first declared output for the parent's
        # default single-output consumption
        out_key = f"{output_prefix}/output.json"
        if primary is not None:
            self.store.put_json(out_key, self.store.get_json(primary))
        self.store.put_json(f"{output_prefix}/_manifest.json",
                            {"output_key": out_key, "outputs": bound_items})
        return {"output_key": out_key}

    async def _run_instance(self, node: PlanNode, ref, state: RunState, run_id: str,
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
        # pass the store config down so the container builds the SAME store the
        # engine uses — uniformly, for every store kind. For S3 the conf is
        # location-independent and passes through verbatim. For a local
        # FileStore the SubprocessRunner rewrites the root to the container's
        # bind-mount path (it owns the mount, so it owns the host->container
        # translation) — so the container always receives one CASCADE_STORE_CONF
        # and builds one store the same way, with no CASCADE_STORE_ROOT special
        # case. The store conf travels on the spec; the runner adjusts it.
        if self.store_conf is not None:
            env["CASCADE_STORE_CONF"] = self.store_conf.to_json()
        spec = RunSpec(run_id=run_id, node_id=node.id, instance_key=ikey.render(),
                       image=ref.image, env=env,
                       runner_config=ref.runner.config, ref_name=ref.name)

        runner = self._runner_for(ref, node)
        # run() is the base poll loop (spawn + poll state until done); it returns
        # the exit code. Uniform across all runner kinds — each only implements
        # spawn + its handle's state.
        exit_code = await runner.run(spec)
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
