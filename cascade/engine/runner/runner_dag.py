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

**It coordinates; it never carries data.** The runner reads no payloads. It resolves
scopes, spawns and awaits.

**It knows nothing about fanning.** A fan is one node deep and opaque: a node with
``scatter`` set is wrapped in a ``FanRunner``, which runs the lanes and gathers them
into one collection at the node's own scope. From here that is indistinguishable from
any other node producing one output, which is why there is no fan state to track and
no per-node result to record — an output always lives at its node's name.
"""
from __future__ import annotations

import asyncio
from typing import Awaitable, Mapping

from cascade.graph import Graph
from cascade.model.dag_node import DagNode
from cascade.model.dependency import Dependency
from cascade.model.runner_kinds import RunnerKind
from cascade.model.runner_overrides import RunnerOverrides
from cascade.plan.plan import Plan
from cascade.store.base import Store
from cascade.store.registry import from_config

from cascade.engine.binding import InputBindings
from cascade.engine.instance_path import InstancePath
from cascade.engine.resolve import ResolveError, resolve_dag_output, resolve_node
from cascade.engine.run_spec import RunSpec
from cascade.engine.runner.registry import RunnerEnv, build_runner
from cascade.engine.runner.runner import Runner
from cascade.engine.runner.runner_coro import RunnerCoroBase
from cascade.engine.runner.runner_fan import FanRunner


class DagRunError(Exception):
    """A node in the dag failed."""


def _store_at(store: Store, scope: tuple[str, ...]) -> Store:
    """A sibling store narrowed by ``scope``, same backend and credentials."""
    config = store.config.subscope(scope)
    _kind, store_cls, _config_cls = from_config(config)
    return store_cls(config)


class DagRunner(RunnerCoroBase):
    def __init__(
        self,
        dag: str,
        plan: Plan,
        env: RunnerEnv | None = None,
        overrides: Mapping[RunnerKind, RunnerOverrides] | None = None,
    ):
        if dag not in plan.node_graphs:
            raise DagRunError(f"no such dag in plan: {dag!r}")
        self.dag = dag
        self.plan = plan
        self.env = env or RunnerEnv()
        self.overrides = dict(overrides or {})

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
            return DagRunner(runnable, self.plan, self.env, self.overrides)
        config = self.plan.run_config.get(runnable)
        if config is None:
            raise DagRunError(f"{runnable!r} is neither a dag nor a configured ref")
        return build_runner(
            config,
            deployment_overrides=self.overrides.get(config.runner),
            env=self.env,
            signature=self.plan.signatures.get(runnable),
        )

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

    async def _run_node(
        self,
        node: DagNode,
        spec: RunSpec,
        path: InstancePath,
        bindings: InputBindings,
    ) -> None:
        child_path = path.child(node.name)
        # the child's reader is this dag's slot: siblings visible, nothing copied
        reader = spec.store_out
        writer = _store_at(spec.store_out, (node.name,)) if spec.store_out else None

        child = RunSpec(
            name=node.runnable_name,
            run_id=spec.run_id,
            node_id=node.name,
            instance_id=str(child_path),
            store_in=reader,
            store_out=writer,
            inputs=bindings,
            args=dict(node.args),
        )
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
        code = await runner.run(child)
        if code != 0:
            raise DagRunError(f"{child_path}: exited {code}")

    async def _run_dag(self, spec: RunSpec) -> int:
        graph = self.graph
        path = InstancePath.parse(spec.instance_id or spec.run_id)
        dag_inputs = spec.inputs or InputBindings()

        for wave in graph.waves():
            dispatches = [
                (graph.node(name), resolve_node(graph.node(name), self.plan, graph, dag_inputs))
                for name in wave
            ]
            # a wave is concurrent by construction: nothing in it depends on
            # anything else in it
            await asyncio.gather(
                *(self._run_node(node, spec, path, b) for node, b in dispatches)
            )

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