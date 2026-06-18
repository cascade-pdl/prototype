"""The naming convention — a single shared source of truth.

Provisioning (setup-time: generate ECR repos, taskdefs, build/push commands) and
the runtime ECS runner (launch-time: reference the taskdef family + image URI)
MUST agree on the names of derived AWS artifacts. They do, by deriving every
name from this one module — given the project name (from cascade.toml) and the
registry URL (from the deployment). If these two sides ever disagreed, the
runner would launch a taskdef that provisioning never created. One module, no
drift.

Conventions:
    ECR repo        <project>/<ref>
    image URI       <registry_url>/<project>/<ref>:<tag>
    local image     <project>-<ref>:dev          (subprocess runner)
    taskdef family  <project>-<ref>
    container name  <ref>                          (must match run_task override)
    log group       /ecs/<project>                 (shared across refs)
    ref context dir refs/<ref>                      (relative to the pipeline)
"""

from __future__ import annotations


def ecr_repository(project: str, ref: str) -> str:
    return f"{project}/{ref}"


def image_uri(registry_url: str, project: str, ref: str, tag: str) -> str:
    return f"{registry_url.rstrip('/')}/{project}/{ref}:{tag}"


def local_image(project: str, ref: str) -> str:
    return f"{project}-{ref}:dev"


def taskdef_family(project: str, ref: str) -> str:
    return f"{project}-{ref}"


def container_name(ref: str) -> str:
    return ref


def log_group(project: str) -> str:
    return f"/ecs/{project}"


def context_dir(ref: str) -> str:
    return f"refs/{ref}"
