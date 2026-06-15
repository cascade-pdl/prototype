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
    RunnerConfig,
    Structure,
    TypesSection,
)


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
    rc = None
    if raw.get("runner_config"):
        c = raw["runner_config"]
        known = {"cpu", "memory", "timeout", "task_definition"}
        rc = RunnerConfig(
            cpu=c.get("cpu"),
            memory=c.get("memory"),
            timeout=c.get("timeout"),
            task_definition=c.get("task_definition"),
            extra={k: v for k, v in c.items() if k not in known},
        )
    return Ref(
        name=raw["name"],
        image=raw["image"],
        runner=raw.get("runner", "subprocess"),
        encoding=raw.get("encoding", "json"),
        runner_config=rc,
        input=[_load_io(i) for i in (raw.get("input") or [])],
        output=[_load_io(o) for o in (raw.get("output") or [])],
    )


def _load_io(raw: dict[str, Any]) -> IoDecl:
    return IoDecl(
        name=raw["name"],
        type=raw["type"],
        encoding=raw.get("encoding"),
        mapping=raw.get("mapping") or {},
    )


def _load_named_dag(raw: dict[str, Any]) -> NamedDag:
    return NamedDag(name=raw["name"], nodes=_load_dag(raw.get("dag") or {}))


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
