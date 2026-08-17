"""Edge validation: do the type expressions on each edge agree?

Because both ends of an edge are declared against the same vocabulary in the same
pipeline, the check is type-expression name + arity equality — no structure
resolution, no subtyping, no registry. (Subtype-compatible edges are a future
choice; today a port's declared type must match exactly.)

Optionally, given a TypeEnv, each declared port's base type is checked for
membership (a declared structure or a known primitive) to catch typos — a set
lookup, not subtyping.

Reuses the edge-resolution helpers from elaborate so the arity logic lives in one
place. Takes prebuilt node_graphs (no rebuild).
"""
from __future__ import annotations

from cascade.graph import Graph
from cascade.model.pipeline import Pipeline
from cascade.model.dag_node import DagNode
from cascade.model.dependency import Dependency
from cascade.plan.signature import Signature, TypeExpr
from cascade.plan.type_env import TypeEnv
from cascade.model.types import check_annotation
from cascade.model.types import Structure
from cascade.plan.elaborate import _NodeInfo, resolve_edge


def validate_edges(
    pipeline: Pipeline,
    node_graphs: dict[str, Graph[DagNode, Dependency]],
    signatures: dict[str, Signature],
    type_env: TypeEnv | None = None,
) -> list[str]:
    errors: list[str] = []

    errors += _check_reserved_names(pipeline)

    if type_env is not None:
        errors += _check_vocabulary(pipeline, type_env)

    for dag in pipeline.dags:
        graph = node_graphs[dag.name]
        dag_inputs = {p.name: TypeExpr.parse(p.type) for p in dag.input}

        info: dict[str, _NodeInfo] = {}
        for node_name in graph.static_order():
            node = graph.node(node_name)
            info[node.name] = _NodeInfo(
                sig=signatures[node.runnable_name],
                fan=node.scatter is not None,
                merge=node.merge,
            )

        for node in dag.nodes:
            node_sig = signatures[node.runnable_name]
            for dep in node.depends_on:
                supplied = resolve_edge(dep, info, dag_inputs)
                port = dep.as_ or dep.field
                if port is None:
                    if len(node_sig.inputs) == 1:
                        (port,) = node_sig.inputs
                    else:
                        errors.append(
                            f"{dag.name}.{node.name}: edge from {dep.node!r} has no "
                            f"'as'/'field' to bind to one of {len(node_sig.inputs)} inputs"
                        )
                        continue
                if port not in node_sig.inputs:
                    errors.append(
                        f"{dag.name}.{node.name}: no input port {port!r} "
                        f"(runs {node.runnable_name!r})"
                    )
                    continue
                # a scattered port consumes one element of the supplied collection
                if node.scatter == port and supplied.depth >= 1:
                    supplied = supplied.element()
                expected = node_sig.inputs[port]
                if not expected.accepts(supplied):
                    errors.append(
                        f"{dag.name}.{node.name}: port {port!r} expects "
                        f"{expected.render()}, got {supplied.render()} from {dep.node!r}"
                    )
    return errors


def _check_type(where: str, declared: str, type_env: TypeEnv) -> list[str]:
    """One declared type: the base must exist, and any annotation must be registered
    and legal for that base."""
    errors: list[str] = []
    t = TypeExpr.parse(declared)
    # what this pipeline declared is the compiler's business...
    if not type_env.is_defined(t.base):
        errors.append(f"{where}: unknown type {t.base!r}")
        return errors
    # ...what a type means is the type system's
    if problem := check_annotation(t.base, t.annotation):
        errors.append(f"{where}: {problem}")
    return errors


def _check_vocabulary(pipeline: Pipeline, type_env: TypeEnv) -> list[str]:
    errors: list[str] = []
    for ref in pipeline.refs:
        for port in (*ref.input, *ref.output):
            errors += _check_type(f"ref {ref.name!r} port {port.name!r}", port.type, type_env)
    for dag in pipeline.dags:
        for port in dag.input:
            errors += _check_type(f"dag {dag.name!r} input {port.name!r}", port.type, type_env)
    # structure fields were never checked, so a typo'd field type -- or an undefined
    # one such as `number` -- was silently accepted while the same typo in a port was
    # caught. Structures compose, so this is where a mistake propagates furthest.
    for structure in pipeline.types.structures:
        for f in structure.fields:
            errors += _check_type(
                f"structure {structure.name!r} field {f.name!r}", f.type, type_env
            )
        errors += _check_field_refinement(structure, type_env)
    return errors


def _check_field_refinement(structure: Structure, type_env: TypeEnv) -> list[str]:
    """A child may *refine* an inherited field, never widen it.

    ``child.image: string<s3-uri>`` over ``parent.image: string`` is safe — anything
    reading the parent's field gets a value that still honours the parent's promise.
    The reverse is not: code expecting an s3 URI would receive an unqualified string.
    """
    errors: list[str] = []
    parent = type_env.structures.get(structure.extends) if structure.extends else None
    if parent is None:
        return errors
    inherited = {f.name: f.type for f in parent.fields}
    for f in structure.fields:
        if f.name not in inherited:
            continue
        want = TypeExpr.parse(inherited[f.name])
        got = TypeExpr.parse(f.type)
        if not want.accepts(got):
            errors.append(
                f"structure {structure.name!r} field {f.name!r}: {got.render()} does "
                f"not refine {want.render()} from {structure.extends!r}"
            )
    return errors

def _check_reserved_names(pipeline: Pipeline) -> list[str]:
    """``$`` is the engine's sigil, so declared names may not start with it.

    It already marks engine-owned names in the DSL (``$input``) and in the store
    (``$in`` for a run's staged inputs, ``$cascade.collection`` for a collection
    descriptor). Reserving it makes those unambiguous by construction rather than by
    convention: in particular a ref's output cannot collide with the descriptor marker,
    which is what lets the store discriminate a collection from data by inspection.
    """
    errors: list[str] = []

    def check(where: str, names: "list[str]") -> None:
        for name in names:
            if name.startswith("$"):
                errors.append(
                    f"{where}: name {name!r} starts with '$', which is reserved for "
                    "engine-owned names"
                )

    for ref in pipeline.refs:
        check(f"ref {ref.name!r}", [p.name for p in (*ref.input, *ref.output)])
        check("refs", [ref.name])
    for dag in pipeline.dags:
        check(f"dag {dag.name!r}", [p.name for p in dag.input])
        check("dags", [dag.name])
        for node in dag.nodes:
            check(f"dag {dag.name!r}", [node.name])
    for structure in pipeline.types.structures:
        check(f"structure {structure.name!r}", [f.name for f in structure.fields])
        check("types", [structure.name])

    return errors