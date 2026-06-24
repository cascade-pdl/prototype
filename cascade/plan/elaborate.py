"""Elaboration: derive a concrete signature for every runnable.

Lives in cascade.plan (not cascade.model) because it is *analysis over* the
model, not part of the data model — which also keeps the dependency direction
clean: plan -> model, never the reverse.

All ordering and cycle detection is delegated to the shared ``Graph``:
  * ``call_graph(pipeline)`` orders runnables (callees before callers),
  * ``node_graph(dag)`` orders nodes within a dag.
No traversal is hand-rolled here.
"""
from __future__ import annotations

from dataclasses import dataclass

from cascade.model.pipeline import Pipeline
from cascade.model.refs import Ref
from cascade.model.dag import Dag
from cascade.model.dag_node import DagNode
from cascade.model.dependency import Dependency
from cascade.plan.build import node_graph, call_graph


class ElaborationError(Exception):
    """A pipeline could not be resolved to concrete signatures."""


@dataclass(frozen=True)
class TypeExpr:
    base: str
    depth: int

    @classmethod
    def parse(cls, s: str) -> "TypeExpr":
        depth = 0
        while s.endswith("[]"):
            s, depth = s[:-2], depth + 1
        return cls(s.strip(), depth)

    def render(self) -> str:
        return self.base + "[]" * self.depth

    def as_collection(self) -> "TypeExpr":
        return TypeExpr(self.base, self.depth + 1)

    def element(self) -> "TypeExpr":
        if self.depth < 1:
            raise ElaborationError(f"cannot take element of non-collection {self.render()!r}")
        return TypeExpr(self.base, self.depth - 1)


@dataclass
class Signature:
    inputs: dict[str, TypeExpr]
    outputs: dict[str, TypeExpr]


@dataclass
class _NodeInfo:
    sig: Signature
    fan: bool


class Elaborator:
    def __init__(self, pipeline: Pipeline):
        self._pipeline = pipeline
        self._refs = {r.name: r for r in pipeline.refs}
        self._dags = {d.name: d for d in pipeline.dags}
        clash = set(self._refs) & set(self._dags)
        if clash:
            raise ElaborationError(f"names used by both a ref and a dag: {sorted(clash)}")
        self._cache: dict[str, Signature] = {}

    # ------------------------------------------------------------------ public
    def elaborate(self) -> "Elaborator":
        """Resolve every runnable, in dependency order. The call graph gives the
        order (callees first) and raises on unknown runnables or dag-call cycles."""
        for name in call_graph(self._pipeline).static_order():
            self._cache[name] = self._resolve(name)
        return self

    def signature(self, name: str) -> Signature:
        if not self._cache:
            self.elaborate()
        if name not in self._cache:
            raise ElaborationError(f"no ref or dag named {name!r}")
        return self._cache[name]

    # ------------------------------------------------------------------- guts
    def _resolve(self, name: str) -> Signature:
        if name in self._refs:
            return self._from_ref(self._refs[name])
        return self._from_dag(self._dags[name])

    def _from_ref(self, ref: Ref) -> Signature:
        return Signature(
            inputs={p.name: TypeExpr.parse(p.type) for p in ref.input},
            outputs={p.name: TypeExpr.parse(p.type) for p in ref.output},
        )

    def _from_dag(self, dag: Dag) -> Signature:
        graph = node_graph(dag)  # shared Graph; raises on intra-dag structural errors
        dag_inputs = {p.name: TypeExpr.parse(p.type) for p in dag.input}

        info: dict[str, _NodeInfo] = {}
        for node_name in graph.static_order():  # graphlib order; raises on node cycle
            node = graph.node(node_name)
            # callee already resolved: call-graph order guarantees it is in cache
            info[node.name] = _NodeInfo(
                sig=self._cache[node.ref_name],
                fan=self._node_fans_out(node, info),
            )
            self._check_node_inputs(node, info, dag_inputs)

        outputs: dict[str, TypeExpr] = {}
        for dep in dag.output:
            t, unclosed_fan = self._resolve_edge(dep, info, dag_inputs)
            if unclosed_fan:
                raise ElaborationError(
                    f"dag {dag.name!r} exports {dep.node}.{dep.field} from a fanned-out "
                    f"node without 'gather' — fan dimension undefined at the boundary"
                )
            outputs[dep.as_ or dep.field or dep.node] = t

        return Signature(inputs=dag_inputs, outputs=outputs)

    # the arity rule (unchanged)
    def _resolve_edge(
        self, dep: Dependency, info: dict[str, _NodeInfo], dag_inputs: dict[str, TypeExpr]
    ) -> tuple[TypeExpr, bool]:
        if dep.is_input:
            if dep.field not in dag_inputs:
                raise ElaborationError(f"$input has no field {dep.field!r}")
            return dag_inputs[dep.field], False

        up = info.get(dep.node)
        if up is None:
            raise ElaborationError(f"dependency on unknown or forward node {dep.node!r}")
        field = dep.field
        if field is None:
            if len(up.sig.outputs) != 1:
                raise ElaborationError(
                    f"edge from {dep.node!r} omits 'field' but it has {len(up.sig.outputs)} outputs"
                )
            (field,) = up.sig.outputs
        if field not in up.sig.outputs:
            raise ElaborationError(f"node {dep.node!r} has no output {field!r}")
        t = up.sig.outputs[field]

        if dep.mode == "gather":
            return t.as_collection(), False
        if dep.mode == "single":
            return t, up.fan
        raise ElaborationError(f"unknown dependency mode {dep.mode!r}")

    def _node_fans_out(self, node: DagNode, info: dict[str, _NodeInfo]) -> bool:
        if node.scatter is not None:
            return True
        for dep in node.depends_on:
            if dep.mode == "single" and not dep.is_input:
                up = info.get(dep.node)
                if up is not None and up.fan:
                    return True
        return False

    def _check_node_inputs(
        self, node: DagNode, info: dict[str, _NodeInfo], dag_inputs: dict[str, TypeExpr]
    ) -> None:
        """Resolve each edge and apply the scatter element check. Full type
        *compatibility* (is-a, via the type registry) remains a deliberate stub."""
        for dep in node.depends_on:
            t, _fan = self._resolve_edge(dep, info, dag_inputs)
            if node.scatter is not None and (dep.as_ or dep.field) == node.scatter:
                if t.depth < 1:
                    raise ElaborationError(
                        f"node {node.name!r} scatters over {node.scatter!r}, "
                        f"but it resolves to non-collection {t.render()!r}"
                    )
            # TODO: unify t against the consuming port type (needs type registry)
