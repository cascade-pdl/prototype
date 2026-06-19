"""Shared CLI helpers: config-file resolution with per-file escalation policies.

Three config sources, each with its own honest escalation policy:

  cascade.toml (project)   args(--project-file) -> ./cascade.toml -> fail IFF the
                           command requires it (some commands don't need a project)
  deployment.yaml          args(--runner-config) -> ./deployment.yaml -> EMPTY
                           fallback (a bare run uses local defaults; never fails
                           here — the reachability check fails later if needed)
  store config             ENV(CASCADE_STORE_CONF) -> deployment.store ->
                           --store file root -> fail. The env wins because, inside
                           an engine-spawned container, the engine set it
                           deliberately; the same `store` verb therefore works
                           unchanged on a laptop (resolves via deployment) and in
                           a node (resolves via env).

The store resolver is intentionally ONE escalation chain shared by the universal
`store` commands, the `node` commands, and (via the deployment branch) `run`/`query`
— so `cascade store fetch` behaves identically wherever it runs.
"""

from __future__ import annotations

import os
from pathlib import Path

import yaml


# --------------------------------------------------------------------------- #
# project config (cascade.toml)
# --------------------------------------------------------------------------- #
def load_project(args, required: bool = False):
    """Resolve the project config: --project-file -> ./cascade.toml. If required
    and absent, fail; otherwise return None."""
    from ..project import ProjectConfig, ProjectError
    path = getattr(args, "project_file", None) or "cascade.toml"
    if not Path(path).exists():
        if required:
            raise SystemExit(
                f"this command requires a project config; none found at '{path}' "
                f"(run `cascade authoring new` to scaffold one)")
        return None
    try:
        return ProjectConfig.load(path)
    except ProjectError as e:
        raise SystemExit(str(e))


# --------------------------------------------------------------------------- #
# deployment config (deployment.yaml)
# --------------------------------------------------------------------------- #
def load_deployment(args):
    """Resolve the deployment: --runner-config -> ./deployment.yaml -> EMPTY.
    Never fails on absence — an empty deployment means local subprocess + file
    store defaults. Returns (DeploymentConfig, source_path_or_None)."""
    from ..runners_config import DeploymentConfig
    path = getattr(args, "runner_config", None)
    if path is None and Path("deployment.yaml").exists():
        path = "deployment.yaml"
    raw = yaml.safe_load(open(path)) if path else None
    return DeploymentConfig.from_dict(raw), path


# --------------------------------------------------------------------------- #
# store resolution — the single escalation chain
# --------------------------------------------------------------------------- #
def _project_scope(args):
    """The project's store scope, if a cascade.toml is resolvable; else None
    (a node-side caller has no project file — its conf is already scoped)."""
    proj = load_project(args, required=False)
    return proj.scope if proj else None


def store_resolve(args, *, allow_env: bool = True):
    """Resolve a Store by escalation:
        1. CASCADE_STORE_CONF env (in-container; already scoped) [if allow_env]
        2. deployment.store, scoped by the project scope (cascade.toml)
        3. --store file root (scoped by the project scope)
        4. default ./_cascade_store file store (scoped)
    Step 1 is already-scoped (the engine baked the project scope into the conf
    before shipping it), so it is NOT scoped again. Steps 2-4 are laptop-side and
    DO scope by the project scope when a cascade.toml is present, so
    `cascade store stage` on a laptop lands in the project's scope. The
    --unscoped flag bypasses project scoping (operate at the store root)."""
    from ..store_config import (
        StoreConf, build_store, StoreKind, FileStoreConfig,
    )
    # 1. env (node / in-container) — already scoped; use verbatim
    if allow_env:
        blob = os.environ.get("CASCADE_STORE_CONF")
        if blob:
            return build_store(StoreConf.from_json(blob))
    # 2-4. laptop-side: build from deployment / --store, then scope by project
    deployment, _ = load_deployment(args)
    if deployment.store is not None and deployment.store.kind != StoreKind.file:
        conf = deployment.store
    else:
        if deployment.store is not None and deployment.store.kind == StoreKind.file:
            root = getattr(args, "store", None) or deployment.store.config.root
        else:
            root = getattr(args, "store", None) or "./_cascade_store"
        conf = StoreConf(kind=StoreKind.file, config=FileStoreConfig(root=root))
    if not getattr(args, "unscoped", False):
        scope = _project_scope(args)
        if scope:
            conf = conf.subscope(scope)
    return build_store(conf)


def store_from_deployment(deployment, args, project=None):
    """Engine-side store: from the deployment, scoped by the project scope, with
    --store overriding a file root. (run/query use this.) Returns (conf, store)
    so the caller can ship the already-scoped conf to nodes."""
    from ..store_config import build_store, StoreKind, FileStoreConfig, StoreConf
    conf = deployment.store
    if conf.kind == StoreKind.file and getattr(args, "store", None):
        conf = StoreConf(kind=StoreKind.file, config=FileStoreConfig(root=args.store))
    scope = project.scope if project else _project_scope(args)
    if scope:
        conf = conf.subscope(scope)
    return conf, build_store(conf)


# --------------------------------------------------------------------------- #
# node env contract
# --------------------------------------------------------------------------- #
NODE_ENV_CONTRACT = [
    "CASCADE_STORE_CONF",
    "CASCADE_RUN_ID",
    "CASCADE_NODE_ID",
    "CASCADE_INSTANCE_KEY",
    "CASCADE_INPUT_KEYS",
    "CASCADE_OUTPUT_PREFIX",
    "CASCADE_MANIFEST_KEY",
]


def assert_node_env(extra: list[str] | None = None) -> list[str]:
    """Return the list of missing/empty node-contract env vars (empty = all present)."""
    required = NODE_ENV_CONTRACT + (extra or [])
    return [k for k in required if not os.environ.get(k)]


# --------------------------------------------------------------------------- #
# misc
# --------------------------------------------------------------------------- #
def parse_inputs(items) -> dict:
    """Parse repeated --input name=key pairs into a dict."""
    inputs = {}
    for item in items or []:
        if "=" not in item:
            raise SystemExit(f"--input must be name=key, got '{item}'")
        name, key = item.split("=", 1)
        inputs[name] = key
    return inputs


def print_report(report) -> bool:
    """Print a ValidationReport's diagnostics; return report.ok."""
    if not report.diagnostics:
        print(f"  {report.phase}: ok")
        return True
    for d in report.diagnostics:
        marker = "ERROR" if d.severity == "error" else "warn "
        print(f"  [{marker}] {d.location}: {d.message}")
    print(f"  {report.phase}: {'ok (with warnings)' if report.ok else 'FAILED'}")
    return report.ok
