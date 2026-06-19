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
def store_resolve(args, *, allow_env: bool = True):
    """Resolve a Store by escalation:
        1. CASCADE_STORE_CONF env (in-container; the engine set it) [if allow_env]
        2. deployment.store (from --runner-config / ./deployment.yaml)
        3. --store file root (local file store)
        4. default ./_cascade_store file store
    Returns the built Store. `allow_env=False` skips step 1 (engine-side callers
    that must use the deployment, not whatever env they happen to have)."""
    from ..store_config import (
        StoreConf, build_store, StoreKind, FileStoreConfig,
    )
    # 1. env (node / in-container)
    if allow_env:
        blob = os.environ.get("CASCADE_STORE_CONF")
        if blob:
            return build_store(StoreConf.from_json(blob))
    # 2. deployment
    deployment, _ = load_deployment(args)
    if deployment.store is not None and deployment.store.kind != StoreKind.file:
        return build_store(deployment.store)
    # 3/4. file store: --store override or default
    if deployment.store is not None and deployment.store.kind == StoreKind.file:
        root = getattr(args, "store", None) or deployment.store.config.root
    else:
        root = getattr(args, "store", None) or "./_cascade_store"
    return build_store(StoreConf(kind=StoreKind.file, config=FileStoreConfig(root=root)))


def store_from_deployment(deployment, args):
    """Engine-side store: from the deployment, with --store overriding a file
    root. (run/query use this — they always start from the loaded deployment.)"""
    from ..store_config import build_store, StoreKind, FileStoreConfig, StoreConf
    if deployment.store.kind == StoreKind.file and getattr(args, "store", None):
        deployment.store = StoreConf(kind=StoreKind.file,
                                     config=FileStoreConfig(root=args.store))
    return build_store(deployment.store)


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
