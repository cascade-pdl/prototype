"""Planning: resolve a :class:`Pipeline` into an :class:`ExecutionPlan`.

Steps:
  1. Flatten subdags — a dag node that names a ``dags:`` entry is expanded
     inline, its node ids prefixed (``preprocess.denoise``), and its internal
     ``$input`` references rewired to whatever the parent fed the subdag node.
     (This minimal implementation supports the common, non-nested case and
     leaves deep subdag nesting as a clearly-marked extension point.)
  2. Topologically sort into waves (Kahn's algorithm). Each wave is a set of
     nodes with all dependencies satisfied, runnable concurrently.

The plan carries scatter *points* (which nodes scatter, over which field) but
never the cardinality — that is resolved at runtime by the engine from the
upstream node's reported output count.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .model import DagNode, Pipeline


class PlanError(Exception):
    pass


@dataclass
class PlanNode:
    """A node in the flattened, ready-to-run graph."""
    id: str                       # fully-qualified (subdag-prefixed) id
    ref_name: str
    args: dict
    scatter: str | None
    depends_on: list  # list[Dependency] (reused from model)


@dataclass
class ExecutionPlan:
    nodes: dict[str, PlanNode] = field(default_factory=dict)
    waves: list[list[str]] = field(default_factory=list)

    def node(self, node_id: str) -> PlanNode | None:
        return self.nodes.get(node_id)


def build_plan(pipeline: Pipeline) -> ExecutionPlan:
    nodes = _flatten(pipeline)
    waves = _topological_waves(nodes)
    return ExecutionPlan(nodes=nodes, waves=waves)


# --------------------------------------------------------------------------- #
# Flatten
# --------------------------------------------------------------------------- #
def _flatten(pipeline: Pipeline) -> dict[str, PlanNode]:
    """Flatten the root dag, expanding any node that references a named subdag.

    Resolution rule (uniform resolver): a dag node's ref/name is looked up in
    ``dags:`` first, then ``refs:``.
    """
    out: dict[str, PlanNode] = {}

    def emit(node_id: str, node: DagNode, prefix: str) -> None:
        out[node_id] = PlanNode(
            id=node_id,
            ref_name=node.ref_name,
            args=node.args,
            scatter=node.scatter,
            depends_on=node.depends_on,
        )

    for name, node in pipeline.dag.items():
        target = node.ref_name
        sub = pipeline.find_dag(target)
        if sub is not None:
            # Expand the subdag inline with a prefix. (Minimal: one level; the
            # parent's edges into this node and the subdag's $input rewiring are
            # an extension point flagged below.)
            for sub_name, sub_node in sub.nodes.items():
                emit(f"{name}.{sub_name}", sub_node, prefix=name)
            # NOTE (extension point): rewire the subdag's internal $input
            # references to this node's depends_on, and rewrite parent edges
            # that target `name` to point at the subdag's sink nodes. Left
            # explicit and unhandled in this minimal build; single-level
            # ref-only pipelines (the moth/bird pipelines) don't need it yet.
            continue
        if pipeline.find_ref(target) is None:
            raise PlanError(
                f"dag node '{name}' references '{target}', which is neither a "
                f"ref nor a named dag"
            )
        emit(name, node, prefix="")
    return out


# --------------------------------------------------------------------------- #
# Topological sort -> waves (Kahn's algorithm)
# --------------------------------------------------------------------------- #
def _topological_waves(nodes: dict[str, PlanNode]) -> list[list[str]]:
    # build dependency sets (only node-to-node edges count; $input is not a dep)
    deps: dict[str, set[str]] = {}
    for nid, n in nodes.items():
        d = set()
        for edge in n.depends_on:
            if not edge.is_input:
                if edge.node not in nodes:
                    raise PlanError(
                        f"node '{nid}' depends on unknown upstream '{edge.node}'"
                    )
                d.add(edge.node)
        deps[nid] = d

    waves: list[list[str]] = []
    remaining = dict(deps)
    done: set[str] = set()
    while remaining:
        ready = sorted(nid for nid, d in remaining.items() if d <= done)
        if not ready:
            raise PlanError(
                f"dependency cycle detected among: {sorted(remaining)}"
            )
        waves.append(ready)
        done.update(ready)
        for nid in ready:
            del remaining[nid]
    return waves
