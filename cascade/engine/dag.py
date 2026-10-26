"""``DagRunner`` — executes one dag by walking its waves.

A ``Runner`` like any other, so a dag is dispatched exactly as a ref is, and a dag
containing a dag is one recursion with no special case. That equivalence is also what
makes distribution a deployment choice later: because a dag execution is a spawnable
unit returning a handle, relocating a subtree to ECS means picking a different runner
for that node, not restructuring the coordinator.

**Store scoping falls out of the recursion.** An instance's writer store is scoped to
its own slot, and its children live directly beneath it — so a child's *reader* is
exactly this dag's *writer*, and a child's writer is that subscoped by the node name.
Siblings are therefore visible to each other with nothing copied, and the dag never
needs the deployment's base config: it derives everything from the store it was handed.

**A dag stages its own inputs.** They are bound relative to the dag's *reader* (its
parent's slot), which its inner nodes cannot see: they read this dag's slot. So on
entry the dag copies each bound input into ``<slot>/$in/`` and rebinds it there, which
is exactly what the executor does for the root. Each boundary reaches one level up,
once, so no instance ever needs a store wider than its dag, at any depth. A fan lane's
scattered element is already staged at that spot and is not copied twice.

**It coordinates; it never carries data.** The runner reads no payloads. It resolves
scopes, spawns and awaits.

**It knows nothing about fanning.** A fan is one node deep and opaque: a node with
``scatter`` set is wrapped in a ``FanRunner``, which runs the lanes and gathers them
into one collection at the node's own scope. From here that is indistinguishable from
any other node producing one output, which is why there is no fan state to track and
no per-node result to record — an output always lives at its node's name.
"""
from __future__ import annotations

from dataclasses import replace
from typing import Awaitable, Iterator

from cascade.graph import Graph
from cascade.model.dag_node import DagNode
from cascade.model.dependency import Dependency
from cascade.plan.plan import Plan
from cascade.store.base import Store
from cascade.store.registry import from_config

from cascade.protocol.binding import INPUTS_SCOPE, InputBindings, OutputDecl, OutputDecls
from cascade.protocol.instance_path import InstancePath
from cascade.engine.resolve import ResolveError, resolve_dag_output, resolve_node
from cascade.protocol.run_spec import RunSpec
from cascade.plan.slice import slice_plan
from cascade.runners.build import RefRunnerBuilder, configure_ref_builder
from cascade.protocol.runner import Runner
from cascade.protocol.coro import RunnerCoroBase
from cascade.engine.fan import FanRunner
from cascade.engine.concurrency import run_all


class DagRunError(Exception):
    """A node in the dag failed."""


def _store_at(store: Store, scope: tuple[str, ...]) -> Store:
    """A sibling store narrowed by ``scope``, same backend and credentials."""
    config = store.config.subscope(scope)
    _kind, store_cls, _config_cls = from_config(config)
    return store_cls(config)


def _same_location(a: Store, a_at: tuple[str, ...], a_key: str,
                   b: Store, b_at: tuple[str, ...], b_key: str) -> bool:
    """Whether two (store, scope, key) triples name the same object."""
    a_raw, b_raw = a.config.encode(), b.config.encode()
    a_full = (*a_raw.pop("scope"), *a_at, a_key)
    b_full = (*b_raw.pop("scope"), *b_at, b_key)
    return a_raw == b_raw and a_full == b_full


class DagRunner(RunnerCoroBase):
    def __init__(
        self,
        dag: str,
        plan: Plan,
        build_ref: "RefRunnerBuilder | None" = None,
    ):
        """Coordination needs the plan and a way to build refs — nothing else.

        It previously took a ``RunnerEnv`` and an overrides map, stored both, threaded
        both through its recursion, and read neither: they existed only to reach
        ``build_runner``. Injecting the builder puts all of that on the other side of a
        callable, so *how* a runner is configured is no longer coordination's business.
        """
        if dag not in plan.node_graphs:
            raise DagRunError(f"no such dag in plan: {dag!r}")
        self.dag = dag
        self.plan = plan
        self.build_ref = build_ref or configure_ref_builder()

    # ------------------------------------------------------------------ Runner
    def _invoke(self, spec: RunSpec) -> Awaitable[int]:
        return self._run_dag(spec)

    # -------------------------------------------------------------- internals
    @property
    def graph(self) -> Graph[DagNode, Dependency]:
        return self.plan.node_graphs[self.dag]

    def _runner_for(self, runnable: str) -> Runner:
        """A ref becomes a spawning runner; a dag becomes another DagRunner. The
        recursion is here and nowhere else."""
        if runnable in self.plan.node_graphs:
            # a child gets only its own subtree. Encapsulation for its own sake -- a subdag
            # cannot reach a sibling's config if the entry is not there -- and because a
            # dag-in-a-container must ship a slice anyway: slicing here makes the local
            # recursion and the remote dispatch the same operation rather than two paths.
            # Cost is once per node dispatch, not per lane; nested dags re-slice per level.
            return DagRunner(runnable, slice_plan(self.plan, runnable), self.build_ref)
        config = self.plan.run_config.get(runnable)
        if config is None:
            raise DagRunError(f"{runnable!r} is neither a dag nor a configured ref")
        # the plan is this runner's own data, so looking up in it is not borrowed
        # knowledge; what stays outside is *how* a runner is built from what we find
        return self.build_ref(runnable, config, self.plan.signatures.get(runnable))

    def _lane_outputs(self, runnable: str) -> dict[str, tuple[tuple[str, ...], str]]:
        """Where each output port lands inside one lane. A ref writes at the lane
        root; a dag writes through its inner nodes, so its alias must be followed."""
        signature = self.plan.signatures.get(runnable)
        if signature is None:
            return {}
        if runnable in self.plan.node_graphs:
            return {
                port: resolve_dag_output(self.plan, runnable, port)
                for port in signature.outputs
            }
        return {port: ((), port) for port in signature.outputs}

    def _node_runner(self, node: DagNode) -> Runner:
        runner = self._runner_for(node.runnable_name)
        if node.scatter is not None:
            # wrap whatever this node runs -- a ref or a whole dag -- and let the fan
            # close at this node's boundary, so downstream sees one collection
            runner = FanRunner(
                child=runner,
                scatter_port=node.scatter,
                outputs=self._lane_outputs(node.runnable_name),
                merge=node.merge,
            )
        return runner

    def _child_spec(
        self,
        node: DagNode,
        spec: RunSpec,
        path: InstancePath,
        bindings: InputBindings,
    ) -> RunSpec:
        child_path = path.child(node.name)
        # the child's reader is this dag's slot: siblings visible, nothing copied
        reader = spec.store_out
        writer = _store_at(spec.store_out, (node.name,)) if spec.store_out else None

        signature = self.plan.signatures.get(node.runnable_name)
        declared = OutputDecls(
            outputs=tuple(
                OutputDecl(port=port, type=port_decl.type, config=port_decl.config)
                for port, port_decl in (signature.outputs.items() if signature else ())
            )
        )
        return RunSpec(
            name=node.runnable_name,
            run_id=spec.run_id,
            node_id=node.name,
            instance_id=str(child_path),
            store_in=reader,
            store_out=writer,
            inputs=bindings,
            outputs=declared,
            args=dict(node.args),
        )

    async def _run_node(
        self,
        node: DagNode,
        spec: RunSpec,
        path: InstancePath,
        bindings: InputBindings,
    ) -> None:
        child = self._child_spec(node, spec, path, bindings)
        code = await self._node_runner(node).run(child)
        if code != 0:
            raise DagRunError(f"{child.instance_id}: exited {code}")

    def _staged(self, spec: RunSpec) -> InputBindings:
        """This dag's inputs as bound once staged: each at ``$in/<port>``.

        Without a reader there is no parent scope: this is the root, and the executor
        has already staged the run's inputs into ``$in``.
        """
        bindings = spec.inputs or InputBindings()
        if spec.store_in is None or not bindings.inputs:
            return bindings
        return InputBindings(
            inputs=tuple(replace(b, scope=(INPUTS_SCOPE,), key=b.port) for b in bindings.inputs)
        )

    def _stage_inputs(self, spec: RunSpec) -> InputBindings:
        """Copy each bound input into this dag's ``$in`` and rebind it there."""
        staged = self._staged(spec)
        if staged is spec.inputs or not staged.inputs:
            return staged
        if spec.store_out is None:
            raise DagRunError(f"dag {self.dag!r} has inputs to stage but no writer store")
        for b, to in zip(spec.inputs.inputs, staged.inputs):
            if not _same_location(spec.store_in, b.scope, b.key, spec.store_out, to.scope, to.key):
                spec.store_in.copy_resolved(
                    b.key, to.key, from_=b.scope, to_=to.scope, dst_store=spec.store_out
                )
        return staged

    def leaves(self, spec: RunSpec) -> Iterator[tuple[Runner, RunSpec]]:
        """Every ref launch this dag would make, with the spec it would get, by the same
        steps as a run but reading and writing nothing."""
        graph = self.graph
        path = InstancePath.parse(spec.instance_id or spec.run_id)
        dag_inputs = self._staged(spec)
        for wave in graph.waves():
            for name in wave:
                node = graph.node(name)
                bindings = resolve_node(node, self.plan, graph, dag_inputs)
                child = self._child_spec(node, spec, path, bindings)
                yield from self._node_runner(node).leaves(child)

    async def _run_dag(self, spec: RunSpec) -> int:
        graph = self.graph
        path = InstancePath.parse(spec.instance_id or spec.run_id)
        dag_inputs = self._stage_inputs(spec)

        for wave in graph.waves():
            dispatches = [
                (graph.node(name), resolve_node(graph.node(name), self.plan, graph, dag_inputs))
                for name in wave
            ]
            # a wave is concurrent by construction: nothing in it depends on
            # anything else in it. What a failure does to the rest of the wave is
            # run_all's decision (fail fast); raising here starts no later wave.
            await run_all(self._run_node(node, spec, path, b) for node, b in dispatches)

        return 0

    # ------------------------------------------------------------------ output
    def output_scopes(self) -> dict[str, tuple[tuple[str, ...], str]]:
        """Where this dag's declared outputs live, relative to its own slot.

        Derived from the plan rather than recorded during the run: a dag's output port
        names an inner node's output, so the bytes are already in place. This is the
        alias — no artifact is written and nothing is copied.
        """
        from cascade.engine.resolve import resolve_dag_output

        out = {}
        for dep in self.plan.dag_outputs.get(self.dag, []):
            port = dep.as_ or dep.field
            if port is None:
                continue
            try:
                out[port] = resolve_dag_output(self.plan, self.dag, port)
            except ResolveError:
                continue
        return out