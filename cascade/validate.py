"""Validation.

``validate_dags`` is the differentiated capability: it checks every edge in the
(flattened) dag against the declared ref contracts, applying the edge's
transform (scatter/gather), and reports type mismatches *before* anything runs.
This is the moment that catches "the classifier's output no longer fits the
tracker's input" at validation time instead of at 2am in production.

``validate_refs`` is thin here: with pre-built images and declared contracts,
there is little to verify locally beyond well-formedness. (Verifying a declared
contract against the actual image — the drift check — is a runtime concern and
an extension point.)
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .model import Pipeline
from .plan import build_plan
from .types import TypeRegistry, check_edge, parse_type, transform_for, TypeError_


@dataclass
class Diagnostic:
    severity: str          # "error" | "warning"
    code: str
    location: str          # human-readable "node 'x', edge from 'y'"
    message: str


@dataclass
class ValidationReport:
    phase: str
    diagnostics: list[Diagnostic] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not any(d.severity == "error" for d in self.diagnostics)

    def error(self, code: str, location: str, message: str) -> None:
        self.diagnostics.append(Diagnostic("error", code, location, message))

    def warn(self, code: str, location: str, message: str) -> None:
        self.diagnostics.append(Diagnostic("warning", code, location, message))


# --------------------------------------------------------------------------- #
# validate-dags
# --------------------------------------------------------------------------- #
def validate_dags(pipeline: Pipeline) -> ValidationReport:
    report = ValidationReport(phase="dags")
    registry = TypeRegistry.from_section(pipeline.types)

    # structural validity first: does the dag resolve + sort?
    try:
        plan = build_plan(pipeline)
    except Exception as e:  # PlanError and friends
        report.error("structure", "dag", str(e))
        return report

    for node_id, node in plan.nodes.items():
        ref = pipeline.find_ref(node.ref_name)
        if ref is None:
            report.error("unknown-ref", f"node '{node_id}'",
                         f"references unknown ref '{node.ref_name}'")
            continue

        downstream_scatters = node.scatter is not None

        for edge in node.depends_on:
            loc = f"node '{node_id}' <- '{edge.node}'"

            # resolve the upstream type
            if edge.is_input:
                inp = pipeline.find_input(edge.field) if edge.field else None
                if inp is None:
                    report.error("unknown-input", loc,
                                 f"$input field '{edge.field}' is not declared")
                    continue
                upstream_type = inp.type
            else:
                up_node = plan.node(edge.node)
                up_ref = pipeline.find_ref(up_node.ref_name) if up_node else None
                if up_ref is None:
                    report.error("unknown-upstream", loc,
                                 f"upstream node/ref '{edge.node}' not found")
                    continue
                up_out = up_ref.output_field(edge.field)
                if up_out is None:
                    report.error(
                        "unknown-output", loc,
                        f"upstream '{edge.node}' has no output field "
                        f"'{edge.field or '(default)'}' (declares: "
                        f"{[o.name for o in up_ref.output]})",
                    )
                    continue
                upstream_type = up_out.type

            # resolve the downstream input port this edge binds to
            down_in = _resolve_input_port(ref, edge.as_)
            if down_in is None:
                report.error(
                    "unknown-binding", loc,
                    f"node '{node_id}' (ref '{ref.name}') has no input matching "
                    f"binding '{edge.as_}' (declares: {[i.name for i in ref.input]})",
                )
                continue

            transform = transform_for(edge.mode, downstream_scatters)
            try:
                warnings = check_edge(registry, upstream_type, transform, down_in.type)
                for w in warnings:
                    report.warn("nullable-narrowing", loc, w.message)
            except TypeError_ as e:
                report.error("type-mismatch", loc, str(e))

    return report


def _resolve_input_port(ref, binding: str | None):
    """Map an edge binding (``--moth-crop`` or a field name) to a ref input."""
    inputs = ref.input
    if not inputs:
        return None
    if binding is None:
        return inputs[0] if len(inputs) == 1 else None
    # normalise "--moth-crop" -> "moth_crop" / "moth-crop" and match by name
    cand = binding.lstrip("-")
    for i in inputs:
        if i.name == cand or i.name == cand.replace("-", "_") or i.name == binding:
            return i
    # also allow exact match against the raw binding
    return next((i for i in inputs if i.name == binding), None)


# --------------------------------------------------------------------------- #
# validate-refs (thin, for pre-built images)
# --------------------------------------------------------------------------- #
def validate_refs(pipeline: Pipeline) -> ValidationReport:
    report = ValidationReport(phase="refs")
    for ref in pipeline.refs:
        loc = f"ref '{ref.name}'"
        if not ref.image:
            report.error("no-image", loc, "no image declared")
        if ref.runner not in {"subprocess", "ecs-task", "local"}:
            report.error("bad-runner", loc, f"unknown runner '{ref.runner}'")
        # well-formedness of declared types
        for io in (*ref.input, *ref.output):
            try:
                parse_type(io.type)
            except TypeError_ as e:
                report.error("bad-type", loc, f"port '{io.name}': {e}")
    # NOTE (extension point): the runtime drift check — pull the image and
    # verify its actual contract matches the declaration — lives in the build/
    # run stage, not here. With pre-built images there is no build to check.
    return report
