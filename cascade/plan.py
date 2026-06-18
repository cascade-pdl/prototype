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
    """A node in the ready-to-run graph."""
    id: str
    ref_name: str
    args: dict
    scatter: str | None
    depends_on: list  # list[Dependency] (reused from model)
    kind: str = "ref"             # "ref" (leaf container) | "dag" (subdag) | "builtin"


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
    """Resolve each dag node to its kind — a leaf ``ref``, a ``builtin``
    (in-process reshaping like collect), or a ``dag`` (subdag). Subdags are NOT
    expanded inline anymore: a subdag node stays a single node, tagged ``dag``,
    and is spawned as a (dag) runner at execution time, running its body in its
    own scope under a child instance key. This replaces the old macro-expansion,
    which couldn't handle $input rewiring / output wiring across the boundary —
    problems that simply don't exist when the subdag runs in its own scope.
    """
    out: dict[str, PlanNode] = {}
    for name, node in pipeline.dag.items():
        target = node.ref_name
        kind = "ref"
        if isinstance(target, str) and target.startswith("builtin:"):
            kind = "builtin"
        elif pipeline.find_dag(target) is not None:
            kind = "dag"
        elif pipeline.find_ref(target) is None:
            raise PlanError(
                f"dag node '{name}' references '{target}', which is neither a "
                f"ref, a named dag, nor a builtin:")
        out[name] = PlanNode(
            id=name,
            ref_name=target,
            args=node.args,
            scatter=node.scatter,
            depends_on=node.depends_on,
            kind=kind,
        )
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
