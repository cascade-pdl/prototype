"""Loading: YAML text/file -> :class:`Pipeline`.

The only IO surface for the model. Accepts the pipeline file shape::

    pipeline:
      input:   [...]
      types:   { structures: [...] }
      refs:    [...]
      dags:    [...]
      dag:     { node_name: { ref, args, scatter, depends_on }, ... }
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .model import (
    DagNode,
    Dependency,
    FieldDecl,
    InputDecl,
    IoDecl,
    NamedDag,
    Pipeline,
    Ref,
    Structure,
    TypesSection,
)
from .runners_config import RunnerKind, RunnerSpec, parse_node_config


class LoadError(Exception):
    pass


def load_pipeline(path: str | Path) -> Pipeline:
    text = Path(path).read_text()
    return load_pipeline_str(text)


def load_pipeline_str(text: str) -> Pipeline:
    raw = yaml.safe_load(text)
    if not isinstance(raw, dict) or "pipeline" not in raw:
        raise LoadError("top-level 'pipeline:' key is required")
    p = raw["pipeline"]
    return Pipeline(
        types=_load_types(p.get("types") or {}),
        input=[_load_input(i) for i in (p.get("input") or [])],
        refs=[_load_ref(r) for r in (p.get("refs") or [])],
        dags=[_load_named_dag(d) for d in (p.get("dags") or [])],
        dag=_load_dag(p.get("dag") or {}),
    )


def _load_types_file(path) -> TypesSection:
    """Load a standalone types file (either bare ``structures:`` or wrapped in
    ``types:``)."""
    raw = yaml.safe_load(Path(path).read_text()) or {}
    section = raw.get("types", raw)   # accept {types: {structures:...}} or {structures:...}
    return _load_types(section or {})


def load_project_pipeline(project, pipeline_path=None) -> Pipeline:
    """Load a pipeline in the context of a ProjectConfig: merge the project's
    ``include_types`` (shared, reusable across pipelines) into the pipeline's own
    types. ``pipeline_path`` defaults to the project's ``pipeline_file``.

    Included types are prepended (so a pipeline may still override/extend them);
    duplicate-name resolution is left to validation.
    """
    pp = pipeline_path or project.resolve(project.pipeline_file)
    pipeline = load_pipeline(str(pp))
    if project.include_types:
        merged = []
        for inc in project.include_types:
            merged.extend(_load_types_file(project.resolve(inc)).structures)
        merged.extend(pipeline.types.structures)
        pipeline.types.structures = merged
    return pipeline


def _load_types(raw: dict[str, Any]) -> TypesSection:
    structures = []
    for s in raw.get("structures") or []:
        structures.append(
            Structure(
                name=s["name"],
                extends=s.get("extends"),
                fields=[FieldDecl(name=f["name"], type=f["type"]) for f in (s.get("fields") or [])],
            )
        )
    return TypesSection(structures=structures)


def _load_input(raw: dict[str, Any]) -> InputDecl:
    return InputDecl(name=raw["name"], type=raw["type"], default=raw.get("default"))


def _load_ref(raw: dict[str, Any]) -> Ref:
    if "image" not in raw:
        raise LoadError(f"ref '{raw.get('name', '?')}' must declare an 'image' (pre-built container)")
    runner_spec = _load_runner(raw)
    return Ref(
        name=raw["name"],
        image=raw["image"],
        runner=runner_spec,
        encoding=raw.get("encoding", "json"),
        input=[_load_io(i) for i in (raw.get("input") or [])],
        output=[_load_io(o) for o in (raw.get("output") or [])],
    )


def _load_runner(raw: dict[str, Any]) -> RunnerSpec:
    """Parse a ref's runner. Accepts:
      runner: subprocess                         (bare kind string)
      runner: {kind: ecs-task, config: {...}}    (structured)
      runner: ecs-task + runner_config: {...}    (kind + sibling config block)
    """
    r = raw.get("runner", "subprocess")
    sibling_cfg = raw.get("runner_config")

    if isinstance(r, str):
        try:
            kind = RunnerKind(r)
        except ValueError:
            raise LoadError(
                f"ref '{raw.get('name','?')}' has unknown runner '{r}'; "
                f"valid: {[k.value for k in RunnerKind]}"
            )
        try:
            cfg = parse_node_config(kind, sibling_cfg)
        except ValueError as e:
            raise LoadError(f"ref '{raw.get('name','?')}': {e}")
        return RunnerSpec(kind=kind, config=cfg)

    if isinstance(r, dict):
        kind_str = r.get("kind")
        try:
            kind = RunnerKind(kind_str)
        except ValueError:
            raise LoadError(
                f"ref '{raw.get('name','?')}' has unknown runner kind '{kind_str}'; "
                f"valid: {[k.value for k in RunnerKind]}"
            )
        cfg_raw = r.get("config") or sibling_cfg
        try:
            cfg = parse_node_config(kind, cfg_raw)
        except ValueError as e:
            raise LoadError(f"ref '{raw.get('name','?')}': {e}")
        return RunnerSpec(kind=kind, config=cfg)

    raise LoadError(f"ref '{raw.get('name','?')}' has invalid runner: {r!r}")


def _load_io(raw: dict[str, Any]) -> IoDecl:
    return IoDecl(
        name=raw["name"],
        type=raw["type"],
        encoding=raw.get("encoding"),
        mapping=raw.get("mapping") or {},
    )


def _load_named_dag(raw: dict[str, Any]) -> NamedDag:
    return NamedDag(
        name=raw["name"],
        nodes=_load_dag(raw.get("dag") or {}),
        inputs=list(raw.get("inputs") or []),
        outputs=list(raw.get("outputs") or []),
    )


def _load_dag(raw: dict[str, Any]) -> dict[str, DagNode]:
    nodes: dict[str, DagNode] = {}
    for name, body in raw.items():
        body = body or {}
        deps = [_load_dep(d) for d in (body.get("depends_on") or [])]
        nodes[name] = DagNode(
            name=name,
            ref=body.get("ref"),
            args=body.get("args") or {},
            scatter=body.get("scatter"),
            depends_on=deps,
        )
    return nodes


def _load_dep(raw: dict[str, Any]) -> Dependency:
    # accept both "node:" (current) and "stage:" (older spec) for the upstream key
    node = raw.get("node") or raw.get("stage")
    if node is None:
        raise LoadError(f"depends_on entry missing 'node': {raw}")
    return Dependency(
        node=node,
        field=raw.get("field"),
        as_=raw.get("as"),
        mode=raw.get("mode", "single"),
        merge=raw.get("merge", "concat"),
    )
