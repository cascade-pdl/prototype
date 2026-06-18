"""Local Docker runner (subprocess): runs a node as a `docker run` container.

Handles the coupled local-docker concerns (current-user mapping, AWS creds
mount, HOME) so they don't have to be passed as raw docker args each run.
"""

from __future__ import annotations

import asyncio

from .base import Runner, Handle, TaskStatus, RunSpec


class SubprocessRunner(Runner):
    """Runs the node as a local Docker container via ``docker run``.

    The store must be reachable from inside the container. Pass ``store_root``
    (the FileStore's host directory) and the runner bind-mounts it at
    ``container_store`` (default ``/store``) and sets ``CASCADE_STORE_ROOT`` so
    the container resolves the engine's relative store keys against the mount.

    Local-docker concerns handled as a coupled unit (because getting them right
    separately is the fiddly part):

    - **User mapping** (``map_current_user``, default True): runs the container
      as the current uid:gid so files it writes into the bind-mounted store are
      owned by you, not root — otherwise the engine (running as you) can't write
      its run-state alongside them. Skipped on platforms without ``os.getuid``
      (Windows) or if disabled.

    - **HOME**: when user-mapping is on, the container's non-root user has no
      usable home, so HOME is set to a writable location (default ``/tmp``) —
      and kept consistent with where AWS creds are mounted, so boto3 finds them.

    - **AWS credentials** (``aws_credentials``): a host ``~/.aws`` is mounted
      read-only into the container at ``container_path`` (default
      ``/tmp/.aws``), and HOME is set to its parent so boto3 discovers it. This
      is the one thing you point at; the mount + HOME coupling is derived.

    ``no_pull`` adds ``--pull=never`` so a locally-built ``:dev`` image isn't
    chased to a registry. ``extra_args`` is the escape hatch for anything else.
    """

    def __init__(self, store_root: str | None = None, container_store: str = "/store",
                 store_mount: str | None = None, no_pull: bool = True,
                 extra_args: list[str] | None = None,
                 map_current_user: bool = True,
                 aws_credentials_host: str | None = None,
                 aws_credentials_container: str = "/tmp/.aws",
                 home: str | None = None):
        self.store_root = store_root
        self.container_store = container_store
        self.store_mount = store_mount
        self.no_pull = no_pull
        self.extra_args = extra_args or []
        self.map_current_user = map_current_user
        self.aws_credentials_host = aws_credentials_host
        self.aws_credentials_container = aws_credentials_container
        self.home = home

    def _build_cmd(self, spec: RunSpec) -> list[str]:
        import os
        cmd = ["docker", "run", "--rm"]
        if self.no_pull:
            cmd += ["--pull", "never"]
        mount = self.store_mount
        if mount is None and self.store_root is not None:
            host = os.path.abspath(self.store_root)
            mount = f"{host}:{self.container_store}"
        if mount:
            cmd += ["-v", mount]
        env = dict(spec.env)
        if self.store_root is not None and "CASCADE_STORE_ROOT" not in env:
            env["CASCADE_STORE_ROOT"] = self.container_store
        mapped_user = False
        if self.map_current_user and hasattr(os, "getuid"):
            cmd += ["--user", f"{os.getuid()}:{os.getgid()}"]
            mapped_user = True
        home = self.home
        if self.aws_credentials_host:
            host_aws = os.path.abspath(os.path.expanduser(self.aws_credentials_host))
            cmd += ["-v", f"{host_aws}:{self.aws_credentials_container}:ro"]
            if home is None:
                home = os.path.dirname(self.aws_credentials_container) or "/tmp"
        if home is None and mapped_user:
            home = "/tmp"
        if home is not None and "HOME" not in env:
            env["HOME"] = home
        for k, v in env.items():
            cmd += ["-e", f"{k}={v}"]
        cmd += self.extra_args
        cmd.append(spec.image)
        return cmd

    def spawn(self, spec: RunSpec) -> Handle:
        # launch the container as an asyncio subprocess and hand back a handle
        # that polls the process state. The launch itself is async, so we kick
        # it off in a small task the handle awaits-into on first poll.
        cmd = self._build_cmd(spec)
        return _SubprocessHandle(cmd)


class _SubprocessHandle(Handle):
    """Polls a docker `docker run` subprocess. Launches on first state() call,
    then tracks completion via an internal wait task so returncode is reliably
    reaped, surfacing the exit code when done."""

    def __init__(self, cmd: list[str]):
        self._cmd = cmd
        self._proc = None
        self._wait_task = None

    async def state(self) -> TaskStatus:
        if self._proc is None:
            self._proc = await asyncio.create_subprocess_exec(*self._cmd)
            self._wait_task = asyncio.create_task(self._proc.wait())
        if self._wait_task.done():
            rc = self._wait_task.result()
            return TaskStatus(running=False, exit_code=rc if rc is not None else 1)
        return TaskStatus(running=True)

    async def await_done(self) -> TaskStatus | None:
        # ensure launched, then await the process directly (no poll latency)
        if self._proc is None:
            self._proc = await asyncio.create_subprocess_exec(*self._cmd)
            self._wait_task = asyncio.create_task(self._proc.wait())
        rc = await self._wait_task
        return TaskStatus(running=False, exit_code=rc if rc is not None else 1)

