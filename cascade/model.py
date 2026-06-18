"""The Cascade data model — the YAML pipeline, parsed into dataclasses.

This is pure data: no resolution, no checking, no execution. Refs are
deliberately minimal here — a ref is a pre-built container ``image`` plus a
declared input/output contract. (Build-from-source, inline code, etc. are
deferred; the protocol treats them as additional ``build`` variants later.)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


# --------------------------------------------------------------------------- #
# Types section
# --------------------------------------------------------------------------- #
@dataclass
class FieldDecl:
    """One field of a structure: a name and a type expression string."""
    name: str
    type: str  # raw type expression, e.g. "float", "string<uuid>", "Detection[]"


@dataclass
class Structure:
    """A named record type. ``extends`` gives single-inheritance (structural
    subtyping); the child has all the parent's fields plus its own."""
    name: str
    fields: list[FieldDecl] = field(default_factory=list)
    extends: str | None = None


@dataclass
class TypesSection:
    structures: list[Structure] = field(default_factory=list)


# --------------------------------------------------------------------------- #
# Inputs
# --------------------------------------------------------------------------- #
@dataclass
class InputDecl:
    name: str
    type: str
    default: Any = None


# --------------------------------------------------------------------------- #
# Refs — minimal: a pre-built image + a typed contract
# --------------------------------------------------------------------------- #
@dataclass
class IoDecl:
    """One named input or output port with a type expression.

    ``encoding`` is the *node-local* serialization this container expects/produces
    for this port (e.g. "json", "csv"). It is a property of the *location* (the
    port), never of the logical type — the same type can be carried in any
    encoding. When set, it overrides the ref-level default encoding for this port.

    ``mapping`` is an optional *type-preserving field rename* between the
    canonical field names (as the type declares them) and the names this
    container uses locally. It maps ``canonical_name -> local_name``. It may ONLY
    relabel; it may not compute or restructure (that would be a node). The
    connection check verifies the rename preserves types.
    """
    name: str
    type: str
    encoding: str | None = None
    mapping: dict[str, str] = field(default_factory=dict)


@dataclass
class Ref:
    """An executable artefact. For now: a pre-built container image plus a
    declared input/output contract.

    ``runner`` is a :class:`~cascade.runners_config.RunnerSpec`: the runner kind
    (subprocess | ecs-task | echo) plus optional *per-node* config (cpu/memory).
    Deployment wiring (which ECS cluster, etc.) is NOT here — it is supplied at
    run time so the pipeline stays portable.

    ``encoding`` is the default node-local serialization for all of this ref's
    ports (overridable per-port via :class:`IoDecl.encoding`). The data plane
    itself always stores the *canonical* encoding (JSON); the node's hooks
    translate between canonical and the node's port encoding at the boundary.

    YAML::

        refs:
          - name: flat-bug
            image: 123456789.dkr.ecr.eu-west-1.amazonaws.com/flat-bug:v3
            runner: ecs-task            # bare kind, or a {kind, config} mapping
            runner_config:              # per-node intrinsic needs (cpu/memory)
              cpu: 2048
              memory: 8192
            encoding: csv
            input:
              - { name: image, type: "io.Image" }
            output:
              - { name: detections, type: "ecology.Detection[]" }
    """
    name: str
    image: str
    runner: "RunnerSpec" = None  # type: ignore[assignment]
    encoding: str = "json"
    input: list[IoDecl] = field(default_factory=list)
    output: list[IoDecl] = field(default_factory=list)

    def __post_init__(self):
        from .runners_config import RunnerSpec, RunnerKind
        if self.runner is None:
            self.runner = RunnerSpec(kind=RunnerKind.subprocess)

    def port_encoding(self, port: IoDecl) -> str:
        """The effective encoding for a port: its own override, else the ref default."""
        return port.encoding or self.encoding

    def output_field(self, name: str | None) -> IoDecl | None:
        """Resolve an output field by name, or the sole output if name is None."""
        if name is None:
            return self.output[0] if len(self.output) == 1 else None
        return next((o for o in self.output if o.name == name), None)


# --------------------------------------------------------------------------- #
# Dag
# --------------------------------------------------------------------------- #
@dataclass
class Dependency:
    """One incoming edge of a dag node.

    ``node`` is the upstream node name, or the literal ``"$input"`` to reference
    a pipeline input. ``field`` selects which output (or which input field, for
    ``$input``). ``as_`` is the binding (a ``--flag`` keyword name). ``mode`` is
    ``single`` (one item) or ``gather`` (collect all upstream items first).
    ``merge`` combines gathered payloads (concat | dict | latest).
    """
    node: str
    field: str | None = None
    as_: str | None = None
    mode: str = "single"      # single | gather
    merge: str = "concat"     # concat | dict | latest

    @property
    def is_input(self) -> bool:
        return self.node == "$input"


@dataclass
class DagNode:
    """A node in a dag. ``ref`` names the ref it runs (defaults to the node's
    own key in the dag). ``scatter`` names an upstream collection field to fan
    out over (one instance per item). ``args`` are static kwargs."""
    name: str
    ref: str | None = None
    args: dict[str, Any] = field(default_factory=dict)
    scatter: str | None = None
    depends_on: list[Dependency] = field(default_factory=list)

    @property
    def ref_name(self) -> str:
        return self.ref or self.name


@dataclass
class NamedDag:
    """A reusable subdag (the ``dags:`` section).

    A subdag is an encapsulated unit: it declares ``inputs`` (its own scope — its
    internal ``$input`` resolves to these, wired by the parent at the call site)
    and ``outputs`` (named bindings to internal nodes, each collapsed to the
    subdag's root so the boundary is cardinality-one — internal scatter does not
    leak out). The parent treats the subdag like a single node.
    """
    name: str
    nodes: dict[str, DagNode] = field(default_factory=dict)
    inputs: list[str] = field(default_factory=list)
    # outputs: list of {name, from_node} — subdag output `name` is internal
    # node `from_node`'s output (collapsed to the subdag root)
    outputs: list[dict] = field(default_factory=list)


# --------------------------------------------------------------------------- #
# Pipeline
# --------------------------------------------------------------------------- #
@dataclass
class Pipeline:
    types: TypesSection = field(default_factory=TypesSection)
    input: list[InputDecl] = field(default_factory=list)
    refs: list[Ref] = field(default_factory=list)
    dags: list[NamedDag] = field(default_factory=list)
    dag: dict[str, DagNode] = field(default_factory=dict)

    def find_ref(self, name: str) -> Ref | None:
        return next((r for r in self.refs if r.name == name), None)

    def find_dag(self, name: str) -> NamedDag | None:
        return next((d for d in self.dags if d.name == name), None)

    def find_input(self, name: str) -> InputDecl | None:
        return next((i for i in self.input if i.name == name), None)
