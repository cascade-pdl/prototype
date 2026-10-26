import asyncio
import json
import os
import re
import secrets
from dataclasses import dataclass, replace

from cascade.protocol.bundle import BUNDLE_ENTRYPOINT, BUNDLE_ROOT, STAGING_ROOT, wrap_args
from cascade.protocol.runner import Runner
from cascade.protocol.run_status import RunStatus
from cascade.runners.process import HandleSubprocess
from cascade.protocol.run_spec import RunSpec, to_env
from cascade.store.base import Store
from cascade.store.file_store import FileConfig
from cascade.store.registry import from_config


CONTAINER_STORE_ROOT = "/cascade/store"
"""Where a host file store is mounted *inside* the container.

A fixed POSIX path rather than the host path repeated, because a host path is not a valid
mount target for a Linux container on Windows (``-v C:\\x:C:\\x`` is meaningless). So the
runner mounts host→here and rewrites the store root in the spec it hands the container.
The translation belongs here: the runner is the only component that knows a container is
involved, and scopes are untouched, so all addressing still resolves.
"""


def container_name(spec: RunSpec) -> str:
    """A name to find this instance's container by: ``cascade.<instance path>.<rand>``.

    The instance path makes it readable in ``docker ps``; the random suffix keeps a
    re-run with the same ``--run-id`` from colliding with a container still stopping.
    """
    where = (spec.instance_id or spec.name).replace("/", ".")
    return f"cascade.{re.sub(r'[^a-zA-Z0-9_.-]', '_', where)}.{secrets.token_hex(3)}"


class HandleDocker(HandleSubprocess):
    """Killing the ``docker run`` client does not stop its container — SIGKILL cannot
    be forwarded — so ``kill`` stops the container by name first, with the same grace
    period, and only then settles the client."""

    def __init__(self, process, name: str):
        super().__init__(process)
        self.name = name

    async def kill(self) -> RunStatus:
        if self._process.returncode is None:
            stop = await asyncio.create_subprocess_exec(
                "docker", "stop", "--time", str(int(self.kill_grace)), self.name,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            # a non-zero exit means no such container: not created yet, or already
            # gone. Either way the client is all that is left to stop.
            await stop.wait()
        return await super().kill()


class DockerImageError(Exception):
    """The image to run is missing, or gives nothing to run."""


@dataclass(frozen=True)
class ImageConfig:
    """What a wrapped run needs from an image's own config.

    Read from the image because overriding the entrypoint (to put cascade first) discards
    the image's own; it is passed on after ``--`` instead. ``id`` is what is run, so the
    container is exactly the image that was inspected, even if its tag moves meanwhile.
    """

    id: str
    entrypoint: tuple[str, ...]
    cmd: tuple[str, ...]


async def inspect_image(image: str, pull: bool) -> ImageConfig:
    """The image's id, entrypoint and default command, pulling it first if allowed."""
    raw = await _docker_json("image", "inspect", image)
    if raw is None and pull:
        await _docker("pull", "--quiet", image)
        raw = await _docker_json("image", "inspect", image)
    if raw is None:
        raise DockerImageError(
            f"image {image!r} is not present locally"
            + ("" if pull else ", and the backend's `no_pull` stops it being pulled")
        )
    config = raw[0].get("Config") or {}
    return ImageConfig(
        id=raw[0]["Id"],
        entrypoint=tuple(config.get("Entrypoint") or ()),
        cmd=tuple(config.get("Cmd") or ()),
    )


async def image_exists(image: str) -> bool:
    """Whether ``image`` is present locally."""
    code, _, _ = await _docker("image", "inspect", image)
    return code == 0


async def _docker(*argv: str) -> tuple[int, bytes, bytes]:
    proc = await asyncio.create_subprocess_exec(
        "docker", *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    out, err = await proc.communicate()
    return proc.returncode, out, err


async def _docker_json(*argv: str):
    code, out, _ = await _docker(*argv)
    return json.loads(out) if code == 0 else None


class RunnerDocker(Runner):
    """``docker run`` of one ref instance.

    **Wrapped** (``bundle`` given): the image needs no cascade inside. The bundle is
    mounted read-only at ``/opt/cascade`` and made the entrypoint; it runs ``cascade node
    wrap -- <the image's entrypoint> <command, or the image's default command>``, which
    stages inputs under ``/cascade/inputs``, runs the tool, and collects
    ``/cascade/outputs``. ``/cascade`` is a tmpfs anyone can write, because the container
    usually runs as the host user (``map_current_user``) and the image owes it nothing.

    **Unwrapped** (no ``bundle``): the image speaks the protocol itself and runs as
    published, with ``command`` (if any) replacing its default command.
    """

    def __init__(
        self,
        image: str,
        no_pull: bool = True,
        extra_args: list[str] | None = None,
        map_current_user: bool = True,
        aws_credentials_dir: str | None = None,
        memory: int | None = None,
        cpu: float | None = None,
        gpu: int = 0,
        command: list[str] | None = None,
        bundle: str | None = None,
        resolved: ImageConfig | None = None,
    ):
        """``memory`` in MiB, ``cpu`` in vCPU, ``gpu`` a count: the ref's resources,
        which on docker are the allocation as well as the minimum."""
        self.image = image
        self.no_pull = no_pull
        self.extra_args = extra_args or []
        self.map_current_user = map_current_user
        self.aws_credentials_dir = aws_credentials_dir
        self.memory = memory
        self.cpu = cpu
        self.gpu = gpu
        self.command = command
        self.bundle = bundle
        self.resolved = resolved
        """The image as resolved once for the run; runs use its id and ask Docker
        nothing first. Without it, a wrapped run inspects the image at every spawn."""

    def _build_cmd(
        self, spec: RunSpec, name: str | None = None, image: ImageConfig | None = None
    ) -> list[str]:
        """The ``docker run`` command line. ``image`` is the inspected image: needed when
        wrapping, and when given the container runs it by id."""
        if self.bundle is not None and image is None:
            raise ValueError("a wrapped run needs the inspected image")
        cmd = ["docker", "run", "--rm"]
        if name is not None:
            cmd += ["--name", name]
        if self.no_pull:
            cmd += ["--pull", "never"]
        if self.map_current_user and hasattr(os, "getuid"):
            cmd += ["--user", f"{os.getuid()}:{os.getgid()}"]

        # A file store lives on the *host*; without a mount the container writes into its
        # own filesystem and the data is discarded when it exits.
        host_roots = _file_store_roots(spec)
        if host_roots:
            # docker creates a missing bind source itself, owned by root, which a mapped
            # user then cannot write to. A file store no longer makes its root on
            # construction, so make it here, as whoever runs the executor.
            os.makedirs(host_roots[0], exist_ok=True)
            cmd += ["-v", f"{host_roots[0]}:{CONTAINER_STORE_ROOT}"]
            spec = _rerooted(spec, CONTAINER_STORE_ROOT)

        env = to_env(spec=spec)

        if self.aws_credentials_dir:
            # HOME is set *only* here, and only because boto looks for ~/.aws. Forcing it
            # otherwise breaks any image that pip-installed as its own user: the packages
            # sit in that user's ~/.local, and a different HOME hides them.
            home = "/tmp" if self.map_current_user and hasattr(os, "getuid") else "/root"
            host_aws = os.path.abspath(os.path.expanduser(self.aws_credentials_dir))
            cmd += ["-v", f"{host_aws}:{home}/.aws:ro"]
            env["HOME"] = home

        # the ref's resources: hard caps, so a task that outgrows them fails here the
        # way it would on ECS
        if self.memory is not None:
            cmd += ["--memory", f"{self.memory}m"]
        if self.cpu is not None:
            cmd += ["--cpus", f"{self.cpu:g}"]
        if self.gpu:
            cmd += ["--gpus", str(self.gpu)]

        for k, v in env.items():
            cmd += ["-e", f"{k}={v}"]

        if self.bundle is None:
            cmd += self.extra_args
            cmd.append(image.id if image is not None else self.image)
            return cmd + list(self.command or ())

        tool = [*image.entrypoint, *(self.command or image.cmd)]
        if not tool:
            raise DockerImageError(
                f"image {self.image!r} has no entrypoint and no default command, and the "
                "ref gives no `command`: there is nothing to run"
            )
        cmd += [
            "--tmpfs", f"{STAGING_ROOT}:mode=1777",
            "-v", f"{self.bundle}:{BUNDLE_ROOT}:ro",
            "--entrypoint", BUNDLE_ENTRYPOINT,
        ]
        cmd += self.extra_args
        cmd.append(image.id)
        return cmd + wrap_args(tool)

    async def spawn(self, spec: RunSpec) -> HandleDocker:
        name = container_name(spec)
        image = self.resolved
        if image is None and self.bundle is not None:
            image = await inspect_image(self.image, pull=not self.no_pull)
        return HandleDocker(
            process=await asyncio.create_subprocess_exec(
                *self._build_cmd(spec=spec, name=name, image=image),
            ),
            name=name,
        )


def _file_store_roots(spec: RunSpec) -> list[str]:
    """Absolute host roots of any file-backed store this spec hands the container.

    At most one in practice: the reader and writer are subscopes of a single deployment
    store, so they share a root. An S3-backed store needs no mount at all, which is why
    this is a development affordance rather than the production path.
    """
    roots: list[str] = []
    for store in (spec.store_in, spec.store_out):
        config = getattr(store, "config", None)
        if not isinstance(config, FileConfig):
            continue
        root = os.path.abspath(config.root)
        if root not in roots:
            roots.append(root)
    return roots


def _reroot(store: Store | None, root: str) -> Store | None:
    """The same store, addressing the same scope, under a different root.

    ``root`` is a path inside the container, so this relies on constructing a store
    having no effect on the host (see ``FileStore``)."""
    if store is None or not isinstance(store.config, FileConfig):
        return store
    config = replace(store.config, root=root)
    _kind, store_cls, _config_cls = from_config(config)
    return store_cls(config)


def _rerooted(spec: RunSpec, root: str) -> RunSpec:
    return replace(
        spec,
        store_in=_reroot(spec.store_in, root),
        store_out=_reroot(spec.store_out, root),
    )
