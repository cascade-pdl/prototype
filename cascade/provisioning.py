"""Authoring + provisioning command implementations.

These are pure computation over the pipeline + project config + deployment —
they emit names and JSON for you to pipe into the AWS CLI / docker; cascade
never calls AWS here. The runtime ECS runner derives the SAME names from the
same ``naming`` module, so provisioning-creates and runner-consumes agree.

Two namespaces:
    authoring     — create/inspect the project (new, list-refs)
    provisioning  — generate infra artifacts (docker build/push, ecs-task gen-*)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from . import naming
from .project import ProjectConfig, ProjectError
from .loader import load_project_pipeline, load_pipeline
from .runners_config import DeploymentConfig, RunnerKind


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _load_deployment(path: str) -> DeploymentConfig:
    p = Path(path)
    if not p.exists():
        sys.stderr.write("provisioning commands require a deployment file\n")
        raise SystemExit(2)
    import yaml
    return DeploymentConfig.from_dict(yaml.safe_load(p.read_text()))


def _project_and_pipeline(args):
    project = ProjectConfig.load(getattr(args, "project_file", "cascade.toml"))
    pipeline = load_project_pipeline(project)
    return project, pipeline


def _find_ref(pipeline, ref_name):
    ref = pipeline.find_ref(ref_name)
    if ref is None:
        sys.stderr.write(f"no ref named '{ref_name}' in the pipeline\n")
        raise SystemExit(2)
    return ref


# --------------------------------------------------------------------------- #
# authoring
# --------------------------------------------------------------------------- #
def _is_remote_image(image: str) -> bool:
    """Heuristic: does this image reference point at a remote registry (needs a
    push)? A registry host appears before the first '/' and looks like a domain
    (has a '.') or has a port (':'). Bare 'name:tag' or 'org/name:tag' (Docker
    Hub style, no domain) is treated as local. This is just a utility heuristic.
    """
    if "/" not in image:
        return False
    first = image.split("/", 1)[0]
    return ("." in first) or (":" in first)


def cmd_authoring_list_refs(args) -> int:
    project, pipeline = _project_and_pipeline(args)
    want = None
    if args.runner:
        try:
            want = RunnerKind(args.runner)
        except ValueError:
            sys.stderr.write(f"unknown runner kind '{args.runner}'\n")
            return 2
    show_fields = None
    if getattr(args, "show_fields", None):
        show_fields = [f.strip() for f in args.show_fields.split(",") if f.strip()]

    for ref in pipeline.refs:
        if want is not None and ref.runner.kind != want:
            continue
        if getattr(args, "show_remotes", False) and not _is_remote_image(ref.image):
            continue
        if not show_fields:
            print(ref.name)            # bare list (keeps the loop-driver contract)
            continue
        # small YAML block: ref name -> requested fields
        print(f"{ref.name}:")
        for f in show_fields:
            if f == "image":
                print(f"  image: {ref.image}")
            elif f == "runner":
                print(f"  runner: {ref.runner.kind.value}")
            elif f == "encoding":
                print(f"  encoding: {ref.encoding}")
            elif f == "remote":
                print(f"  remote: {str(_is_remote_image(ref.image)).lower()}")
            else:
                sys.stderr.write(f"unknown field '{f}' (image, runner, encoding, remote)\n")
                return 2
    return 0


def cmd_authoring_suggest_image_name(args) -> int:
    """Suggest a reasonable image name for a ref — ADVISORY ONLY. It ignores the
    ref's current `image` entirely and computes a name from the conventions, so
    you can paste it into the ref (or correct an off-convention one). The tag
    comes from git (short commit) if available, else the project version.
    """
    project, pipeline = _project_and_pipeline(args)
    ref = _find_ref(pipeline, args.ref_name)
    tag = args.tag or _suggest_tag(project)
    # remote (ECR) suggestion if a registry is configured, else a local tag
    deployment = None
    try:
        deployment = _load_deployment(args.deployment_file)
    except SystemExit:
        deployment = None
    reg = (deployment.ecs.registry_url
           if (deployment and deployment.ecs and deployment.ecs.registry_url) else None)
    if reg:
        print(naming.image_uri(reg, project.name, ref.name, tag))
    else:
        print(f"{project.name}-{ref.name}:{tag}")
    return 0


def _suggest_tag(project) -> str:
    """Suggest an image tag: short git commit if in a repo, else project version."""
    import subprocess
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(project.root), capture_output=True, text=True, timeout=3)
        if out.returncode == 0 and out.stdout.strip():
            return out.stdout.strip()
    except Exception:
        pass
    return project.version


_SCAFFOLD_PIPELINE = """\
# Minimal starter pipeline — fully functional once you build the image.
# One ref (greet) runs locally via the subprocess runner against a local file
# store, reads the input, and writes a greeting file to the data plane.

pipeline:

  input:
    - name: subject
      type: string

  refs:
    - name: greet
      image: {project}-greet:dev
      runner: subprocess
      encoding: json
      input:
        - {{ name: subject, type: string }}
      output:
        - {{ name: greeting, type: string }}

  dag:
    say:
      ref: greet
      depends_on:
        - node: $input
          field: subject
          as: "--subject"
"""

_SCAFFOLD_DOCKERFILE = """\
# Minimal ref image. The entrypoint reads CASCADE_* env, writes its output and a
# manifest to the data plane (here: a plain "hello world" file plus the
# pipeline's declared greeting output).
FROM python:3.11-slim
WORKDIR /app
COPY entrypoint.py /app/entrypoint.py
ENTRYPOINT ["python", "/app/entrypoint.py"]
"""

_SCAFFOLD_ENTRYPOINT = '''\
"""Trivial ref: writes a hello-world file and the declared greeting output."""
import json, os

op = os.environ["CASCADE_OUTPUT_PREFIX"]
manifest_key = os.environ["CASCADE_MANIFEST_KEY"]
root = os.environ.get("CASCADE_STORE_ROOT", "/store")


def put(key, data: bytes):
    path = os.path.join(root, key)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)


# the obligatory artifact
put(f"{op}/hello.txt", b"hello world\\n")

# the declared 'greeting' output + manifest
out_key = f"{op}/output.json"
put(out_key, json.dumps("hello world").encode())
put(manifest_key, json.dumps({"output_key": out_key}).encode())
print("[greet] wrote hello world")
'''

_SCAFFOLD_DEPLOYMENT = """\
# Local deployment: subprocess runner + a local file store. No AWS needed.
# (For ECS, add an `ecs:` runners block and switch the store to `kind: s3`.)

runners:
  subprocess:
    container_store: /store
    no_pull: true

store:
  kind: file
  # root is supplied at run time via --store, or defaults under the project
"""

_SCAFFOLD_GITIGNORE = "_cascade_store/\n__pycache__/\n*.pyc\n"

_SCAFFOLD_README = """\
# {name}

{description}

A cascade project (YAML + Docker). Build the ref image, then run locally:

```bash
docker build -t {name}-greet:dev refs/greet
mkdir -p _cascade_store
echo '"world"' > _cascade_store/subject.json
cascade run pipeline.yaml \\
    --store _cascade_store \\
    --input subject=subject.json \\
    --run-id demo01 \\
    --runner-config deployment.yaml
```
"""


def cmd_authoring_new(args) -> int:
    name_arg = args.name
    if name_arg == ".":
        target = Path.cwd()
        project_name = target.name
        if not args.yes:
            sys.stderr.write(
                f"Create a new cascade project '{project_name}' in the current "
                f"directory? [y/N] ")
            sys.stderr.flush()
            resp = sys.stdin.readline().strip().lower()
            if resp not in ("y", "yes"):
                sys.stderr.write("aborted\n")
                return 1
    else:
        target = Path(name_arg)
        project_name = target.name

    # validate the name is registry-safe (it becomes ECR repo / taskdef names)
    try:
        ProjectConfig.validate_name(project_name)
    except ProjectError as e:
        sys.stderr.write(str(e) + "\n")
        return 2

    # STRICT: refuse a non-empty target, no matter what
    if target.exists() and any(target.iterdir()):
        sys.stderr.write(
            f"refusing to scaffold into '{target}': directory is not empty\n")
        return 2
    target.mkdir(parents=True, exist_ok=True)

    (target / "cascade.toml").write_text(
        f'[cascade-project]\n'
        f'name = "{project_name}"\n'
        f'version = "0.0.1"\n'
        f'maintainers = []\n'
        f'description = "{project_name}"\n'
        f'pipeline_file = "pipeline.yaml"\n'
    )
    (target / "pipeline.yaml").write_text(_SCAFFOLD_PIPELINE.format(project=project_name))
    (target / "deployment.yaml").write_text(_SCAFFOLD_DEPLOYMENT)
    (target / ".gitignore").write_text(_SCAFFOLD_GITIGNORE)
    (target / "README.md").write_text(
        _SCAFFOLD_README.format(name=project_name, description=project_name))
    refdir = target / "refs" / "greet"
    refdir.mkdir(parents=True)
    (refdir / "Dockerfile").write_text(_SCAFFOLD_DOCKERFILE)
    (refdir / "entrypoint.py").write_text(_SCAFFOLD_ENTRYPOINT)

    print(f"created cascade project '{project_name}' in {target}")
    return 0


# --------------------------------------------------------------------------- #
# provisioning: docker
# --------------------------------------------------------------------------- #
def cmd_prov_docker_build_args(args) -> int:
    project, pipeline = _project_and_pipeline(args)
    ref = _find_ref(pipeline, args.ref_name)
    # the ref's declared image is authoritative — build/tag exactly what the ref
    # says runs (no derivation). Use `suggest-image-name` if you want help
    # choosing that value; this command just honours it.
    ctx = project.resolve(naming.context_dir(ref.name))
    print(f"-t {ref.image} {ctx}")
    return 0


def cmd_prov_docker_push_commands(args) -> int:
    project, pipeline = _project_and_pipeline(args)
    ref = _find_ref(pipeline, args.ref_name)
    if not _is_remote_image(ref.image):
        sys.stderr.write(
            f"ref '{ref.name}' image '{ref.image}' is not a remote reference; "
            f"nothing to push\n")
        return 2
    print(f"docker push {ref.image}")
    return 0


# --------------------------------------------------------------------------- #
# provisioning: ecs-task
# --------------------------------------------------------------------------- #
def cmd_prov_ecs_gen_log(args) -> int:
    project, _ = _project_and_pipeline(args)
    print(naming.log_group(project.name))
    return 0


def cmd_prov_ecs_gen_repository(args) -> int:
    project, pipeline = _project_and_pipeline(args)
    ref = _find_ref(pipeline, args.ref_name)
    print(naming.ecr_repository(project.name, ref.name))
    return 0


def cmd_prov_ecs_gen_taskdef(args) -> int:
    project, pipeline = _project_and_pipeline(args)
    ref = _find_ref(pipeline, args.ref_name)
    deployment = _load_deployment(args.deployment_file)
    if ref.runner.kind != RunnerKind.ecs_task:
        sys.stderr.write(f"ref '{ref.name}' is not an ecs-task ref\n")
        return 2
    ecs = deployment.ecs
    if ecs is None:
        sys.stderr.write("deployment has no ecs runners block\n")
        return 2
    # authoritative: the taskdef pins exactly the ref's declared image
    image = ref.image
    cfg = ref.runner.config
    cpu = getattr(cfg, "cpu", None)
    memory = getattr(cfg, "memory", None)
    region = ecs.region or "us-east-1"
    taskdef = {
        "family": naming.taskdef_family(project.name, ref.name),
        "requiresCompatibilities": ["FARGATE"],
        "networkMode": "awsvpc",
        "cpu": str(cpu or 256),
        "memory": str(memory or 512),
        "containerDefinitions": [{
            "name": naming.container_name(ref.name),
            "image": image,
            "essential": True,
            "logConfiguration": {
                "logDriver": "awslogs",
                "options": {
                    "awslogs-group": ecs.log_group or naming.log_group(project.name),
                    "awslogs-region": region,
                    "awslogs-stream-prefix": ref.name,
                },
            },
        }],
    }
    if ecs.execution_role:
        taskdef["executionRoleArn"] = ecs.execution_role
    if ecs.task_role:
        taskdef["taskRoleArn"] = ecs.task_role
    print(json.dumps(taskdef, indent=2))
    return 0


# --------------------------------------------------------------------------- #
# CLI registration
# --------------------------------------------------------------------------- #
def add_authoring_subcommands(sub):
    a = sub.add_parser("authoring", help="create and inspect cascade projects")
    asub = a.add_subparsers(dest="authoring_cmd", required=True)

    lr = asub.add_parser("list-refs", help="list pipeline refs (optionally by runner)")
    lr.add_argument("--runner", default=None, help="filter by runner kind (e.g. ecs-task)")
    lr.add_argument("--show-remotes", action="store_true",
                    help="only refs whose image is a remote reference (needs push)")
    lr.add_argument("--show-fields", default=None,
                    help="comma-separated fields to show as YAML (image,runner,encoding,remote)")
    lr.add_argument("--project-file", default="cascade.toml")
    lr.set_defaults(func=cmd_authoring_list_refs)

    si = asub.add_parser("suggest-image-name",
                         help="suggest an image name for a ref (advisory; ignores ref.image)")
    si.add_argument("--ref-name", required=True)
    si.add_argument("--tag", default=None, help="override the suggested tag (default: git short sha or project version)")
    si.add_argument("--project-file", default="cascade.toml")
    si.add_argument("--deployment-file", default="deployment.yaml")
    si.set_defaults(func=cmd_authoring_suggest_image_name)

    nw = asub.add_parser("new", help="scaffold a new cascade project")
    nw.add_argument("name", help="project name (new folder), or '.' for the current dir")
    nw.add_argument("--yes", "-y", action="store_true", help="skip confirmation for '.'")
    nw.set_defaults(func=cmd_authoring_new)


def add_provisioning_subcommands(sub):
    p = sub.add_parser("provisioning", help="generate infra artifacts (docker, ecs-task)")
    psub = p.add_subparsers(dest="prov_target", required=True)

    # docker — build/push honour the ref's declared image verbatim
    d = psub.add_parser("docker", help="docker build/push artifacts")
    dsub = d.add_subparsers(dest="docker_cmd", required=True)
    for cmd, fn, helptext in [
        ("build-args", cmd_prov_docker_build_args, "args for `docker build $(...)` (uses ref.image)"),
        ("push-commands", cmd_prov_docker_push_commands, "push command for a remote-image ref"),
    ]:
        c = dsub.add_parser(cmd, help=helptext)
        c.add_argument("--ref-name", required=True)
        c.add_argument("--project-file", default="cascade.toml")
        c.set_defaults(func=fn)

    # ecs-task
    e = psub.add_parser("ecs-task", help="ECS task provisioning artifacts")
    esub = e.add_subparsers(dest="ecs_cmd", required=True)

    gl = esub.add_parser("gen-log", help="generate the log group name")
    gl.add_argument("--project-file", default="cascade.toml")
    gl.set_defaults(func=cmd_prov_ecs_gen_log)

    gr = esub.add_parser("gen-repository", help="generate the ECR repo name")
    gr.add_argument("--ref-name", required=True)
    gr.add_argument("--project-file", default="cascade.toml")
    gr.set_defaults(func=cmd_prov_ecs_gen_repository)

    gt = esub.add_parser("gen-taskdef", help="generate a task definition JSON (uses ref.image)")
    gt.add_argument("--ref-name", required=True)
    gt.add_argument("--project-file", default="cascade.toml")
    gt.add_argument("--deployment-file", default="deployment.yaml")
    gt.set_defaults(func=cmd_prov_ecs_gen_taskdef)
