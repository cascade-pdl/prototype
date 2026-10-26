import asyncio
import ipaddress
import os
import re
import secrets
from dataclasses import replace
from urllib.parse import urlsplit, urlunsplit

from cascade.docker import containers, images
from cascade.docker.images import ImageConfig
from cascade.docker.network import Network
from cascade.protocol.bundle import BUNDLE_ENTRYPOINT, BUNDLE_ROOT, STAGING_ROOT, wrap_args
from cascade.protocol.runner import Runner
from cascade.protocol.run_status import RunStatus
from cascade.runners.credentials import ContainerCredentials
from cascade.runners.process import HandleSubprocess
from cascade.protocol.run_spec import Launch, RunSpec, check_launch, to_env
from cascade.store.base import Store
from cascade.store.file_store import FileConfig
from cascade.store.registry import from_config
from cascade.store.s3_store import S3Config


CONTAINER_STORE_ROOT = "/cascade/store"
"""Where a host file store is mounted *inside* the container.

A fixed POSIX path rather than the host path repeated, because a host path is not a valid
mount target for a Linux container on Windows (``-v C:\\x:C:\\x`` is meaningless). So the
runner mounts host→here and rewrites the store root in the spec it hands the container.
The translation belongs here: the runner is the only component that knows a container is
involved, and scopes are untouched, so all addressing still resolves.
"""

CONTAINER_NETWORK = Network(name="cascade", subnet="169.254.170.0/24", gateway="169.254.170.2")
"""The network local docker containers run on.

Its gateway, the host's side of the bridge, is ECS's credentials address: botocore fetches
container credentials over plain http only from loopback or that address, and this way the
executor serves them from the host (``cascade.runners.credentials``). AWS's own local ECS
emulation uses the same subnet. The gateway is also how a container reaches anything else
the host serves, such as a local S3 service: a store endpoint on the host's loopback is
rewritten to it.
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
            # no such container means not created yet, or already gone: either way the
            # client is all that is left to stop
            await containers.stop(self.name, self.kill_grace)
        return await super().kill()


class DockerImageError(Exception):
    """The image to run is missing, or gives nothing to run."""


async def inspect_image(image: str, pull: bool) -> ImageConfig:
    """The image's id, entrypoint and default command, pulling it first if allowed."""
    try:
        return await images.inspect_image(image, pull=pull)
    except images.ImageNotFound:
        raise DockerImageError(
            f"image {image!r} is not present locally"
            + ("" if pull else ", and the backend's `no_pull` stops it being pulled")
        ) from None


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
        memory: int | None = None,
        cpu: float | None = None,
        gpu: int = 0,
        command: list[str] | None = None,
        bundle: str | None = None,
        resolved: ImageConfig | None = None,
        network: Network | None = None,
        credentials: ContainerCredentials | None = None,
    ):
        """``memory`` in MiB, ``cpu`` in vCPU, ``gpu`` a count: the ref's resources,
        which on docker are the allocation as well as the minimum."""
        self.image = image
        self.no_pull = no_pull
        self.extra_args = extra_args or []
        self.map_current_user = map_current_user
        self.memory = memory
        self.cpu = cpu
        self.gpu = gpu
        self.command = command
        self.bundle = bundle
        self.resolved = resolved
        """The image as resolved once for the run; runs use its id and ask Docker
        nothing first. Without it, a wrapped run inspects the image at every spawn."""
        self.network = network
        """Joined by the container; a store endpoint on the host's loopback is reached at
        its gateway."""
        self.credentials = credentials
        """Where the container fetches its AWS credentials. Not part of the launch: on
        ECS the agent sets its own, so they count towards no budget."""

    def launch_for(self, spec: RunSpec, image: ImageConfig | None = None) -> Launch | None:
        """The container's env and what follows the image on the command line.

        ``image`` defaults to the one resolved for the run. Unknown (``None``) for a
        wrapped run whose image is not resolved yet: the command comes from the image.
        """
        image = image or self.resolved
        env = to_env(spec=self._as_seen_inside(spec))

        if self.bundle is None:
            return Launch(env=env, command=list(self.command or ()))
        if image is None:
            return None
        tool = [*image.entrypoint, *(self.command or image.cmd)]
        if not tool:
            raise DockerImageError(
                f"image {self.image!r} has no entrypoint and no default command, and the "
                "ref gives no `command`: there is nothing to run"
            )
        return Launch(env=env, command=wrap_args(tool))

    def _build_cmd(
        self, spec: RunSpec, name: str | None = None, image: ImageConfig | None = None
    ) -> list[str]:
        """The ``docker run`` command line. ``image`` is the inspected image: needed when
        wrapping, and when given the container runs it by id. Refused if its env and
        command are over the spec budget."""
        if self.bundle is not None and image is None:
            raise ValueError("a wrapped run needs the inspected image")
        launch = self.launch_for(spec, image)
        check_launch(launch, who=spec.instance_id or spec.name)
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

        if self.network is not None:
            cmd += ["--network", self.network.name]

        # the ref's resources: hard caps, so a task that outgrows them fails here the
        # way it would on ECS
        if self.memory is not None:
            cmd += ["--memory", f"{self.memory}m"]
        if self.cpu is not None:
            cmd += ["--cpus", f"{self.cpu:g}"]
        if self.gpu:
            cmd += ["--gpus", str(self.gpu)]

        for k, v in launch.env.items():
            cmd += ["-e", f"{k}={v}"]
        # by name only: the values come from the client's environment (``spawn``), so the
        # token is not on a command line anyone can read
        for k in self.credentials.env() if self.credentials else ():
            cmd += ["-e", k]

        if self.bundle is None:
            cmd += self.extra_args
            cmd.append(image.id if image is not None else self.image)
            return cmd + list(launch.command)

        cmd += [
            "--tmpfs", f"{STAGING_ROOT}:mode=1777",
            "-v", f"{self.bundle}:{BUNDLE_ROOT}:ro",
            "--entrypoint", BUNDLE_ENTRYPOINT,
        ]
        cmd += self.extra_args
        cmd.append(image.id)
        return cmd + list(launch.command)

    async def spawn(self, spec: RunSpec) -> HandleDocker:
        name = container_name(spec)
        image = self.resolved
        if image is None and self.bundle is not None:
            image = await inspect_image(self.image, pull=not self.no_pull)
        env = {**os.environ, **self.credentials.env()} if self.credentials else None
        return HandleDocker(
            process=await asyncio.create_subprocess_exec(
                *self._build_cmd(spec=spec, name=name, image=image), env=env,
            ),
            name=name,
        )

    def _as_seen_inside(self, spec: RunSpec) -> RunSpec:
        """The spec with its stores addressed as the container sees them: a host file
        store at its mount point, a host-local S3 endpoint at the network's gateway."""
        def inside(store: Store | None) -> Store | None:
            config = getattr(store, "config", None)
            if isinstance(config, FileConfig):
                config = replace(config, root=CONTAINER_STORE_ROOT)
            elif isinstance(config, S3Config) and self.network is not None:
                config = replace(
                    config, endpoint=_off_loopback(config.endpoint, self.network.gateway)
                )
            else:
                return store
            # constructing a store has no effect on the host (see ``FileStore``)
            _kind, store_cls, _config_cls = from_config(config)
            return store_cls(config)

        return replace(spec, store_in=inside(spec.store_in), store_out=inside(spec.store_out))


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


def _off_loopback(url: str | None, host: str) -> str | None:
    """``url`` with a loopback host (``localhost``, ``127.0.0.0/8``, ``::1``) replaced by
    ``host``; any other URL unchanged. Inside a container loopback is the container."""
    if url is None:
        return None
    parts = urlsplit(url)
    name = parts.hostname or ""
    try:
        loopback = name == "localhost" or ipaddress.ip_address(name).is_loopback
    except ValueError:
        loopback = False
    if not loopback:
        return url
    netloc = host if parts.port is None else f"{host}:{parts.port}"
    return urlunsplit(parts._replace(netloc=netloc))
