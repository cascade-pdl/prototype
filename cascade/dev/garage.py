"""A local S3 store for development: garage, in docker, with fixed values.

Fixed so the deployment that points at it is a constant, committed beside a project with
no secrets in it (``STORE``); only the keys vary per machine, and they come from the
environment like any AWS identity (``env_lines``).

Not a compose file because two steps are not containers: the cascade network must exist
before garage can be published on its gateway (``CONTAINER_NETWORK``, defined once, in
the package), and a fresh garage needs its layout, bucket and key made through its own
CLI, which a compose service cannot run (the image has no shell).

Published on the host's loopback for the executor, and on the network's gateway for
containers, which reach a loopback endpoint there; not on every interface. Data lives in
a docker volume, the config and keys in a state directory, so ``down`` then ``up`` keeps
both.
"""
from __future__ import annotations

import asyncio
import os
import re
import secrets
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

from cascade.docker.network import ensure_network
from cascade.runners.docker import CONTAINER_NETWORK

IMAGE = "dxflrs/garage@sha256:4c9b34c113e61358466e83fd6e7d66e6d18657ede14b776eb78a93ee8da7cf6a"
"""v2.1.0, pinned by digest."""
BUCKET = "cascade"
REGION = "garage"
PORT = 3900

CONFIG = """\
metadata_dir = "/var/lib/garage/meta"
data_dir = "/var/lib/garage/data"
db_engine = "sqlite"
replication_factor = 1
rpc_bind_addr = "[::]:3901"
rpc_public_addr = "127.0.0.1:3901"
rpc_secret = "{rpc_secret}"

[s3_api]
s3_region = "{region}"
api_bind_addr = "[::]:3900"
"""


class GarageError(Exception):
    """Garage could not be started or set up."""


def _default_state() -> Path:
    base = os.environ.get("XDG_STATE_HOME") or Path.home() / ".local" / "state"
    return Path(base, "cascade", "garage")


@dataclass(frozen=True)
class Instance:
    """One garage. The defaults are the development one; tests make throwaway ones."""

    name: str = "cascade-garage"
    port: int = PORT
    volume: str | None = "cascade-garage"
    """Docker volume for its data; ``None`` keeps data in the container only."""
    state: Path = field(default_factory=_default_state)
    """Its config and keys."""

    @property
    def endpoint(self) -> str:
        return f"http://localhost:{self.port}"

    def store(self, scope: list[str] | None = None) -> dict:
        """A deployment's ``store:`` block for it."""
        config = {"bucket": BUCKET, "region": REGION, "endpoint": self.endpoint}
        return {"kind": "s3", "config": {**config, "scope": list(scope or [])}}


DEV = Instance()


@dataclass(frozen=True)
class Keys:
    key_id: str
    secret: str

    def env(self) -> dict[str, str]:
        return {"AWS_ACCESS_KEY_ID": self.key_id, "AWS_SECRET_ACCESS_KEY": self.secret}


def env_lines(keys: Keys) -> list[str]:
    """Shell lines making ``keys`` this shell's AWS identity. A leftover session token
    would be sent with them and refused, so it is unset."""
    return [f"export {k}={v}" for k, v in keys.env().items()] + ["unset AWS_SESSION_TOKEN"]


def keys(instance: Instance = DEV) -> Keys | None:
    """The instance's keys, once ``up`` has made them."""
    path = instance.state / "keys"
    if not path.is_file():
        return None
    key_id, secret = path.read_text().split()
    return Keys(key_id, secret)


def up(instance: Instance = DEV) -> Keys:
    """Start the instance and set it up; whatever already exists is kept."""
    asyncio.run(ensure_network(CONTAINER_NETWORK))  # its gateway must exist to publish on
    config, found = _state(instance)
    running = _docker("container", "inspect", "-f", "{{.State.Running}}", instance.name)
    if running.returncode:
        _run_container(instance, config)
    elif running.stdout.strip() != "true":
        _check(_docker("start", instance.name), f"starting {instance.name}")
    node = _wait(instance)
    if "NO ROLE ASSIGNED" in _garage(instance, "status"):
        _garage(instance, "layout", "assign", "-z", "dc1", "-c", "1G", node)
        _garage(instance, "layout", "apply", "--version", "1")
    if not _garage_ok(instance, "bucket", "info", BUCKET):
        _garage(instance, "bucket", "create", BUCKET)
    if not _garage_ok(instance, "key", "info", found.key_id):
        _garage(instance, "key", "import", "--yes", "-n", "cascade", found.key_id, found.secret)
    _garage(instance, "bucket", "allow", "--read", "--write", "--owner", BUCKET,
            "--key", found.key_id)
    return found


def down(instance: Instance = DEV, purge: bool = False) -> None:
    """Remove the container; with ``purge``, its data and keys too."""
    _docker("rm", "-f", instance.name)
    if purge:
        if instance.volume:
            _docker("volume", "rm", instance.volume)
        shutil.rmtree(instance.state, ignore_errors=True)


def _state(instance: Instance) -> tuple[Path, Keys]:
    """The config file and keys, made on first use and kept."""
    instance.state.mkdir(parents=True, exist_ok=True)
    config = instance.state / "garage.toml"
    if not config.is_file():
        config.write_text(CONFIG.format(rpc_secret=secrets.token_hex(32), region=REGION))
        config.chmod(0o644)  # read by the container's own user
    found = keys(instance)
    if found is None:
        found = Keys("GK" + secrets.token_hex(12), secrets.token_hex(32))
        path = instance.state / "keys"
        path.write_text(f"{found.key_id} {found.secret}\n")
        path.chmod(0o600)
    return config, found


def _run_container(instance: Instance, config: Path) -> None:
    argv = [
        "run", "-d", "--name", instance.name,
        "-p", f"127.0.0.1:{instance.port}:3900",
        "-p", f"{CONTAINER_NETWORK.gateway}:{instance.port}:3900",
        "-v", f"{config}:/etc/garage.toml:ro",
    ]
    if instance.volume:
        argv += ["-v", f"{instance.volume}:/var/lib/garage"]
    _check(_docker(*argv, IMAGE), f"starting garage as {instance.name!r}")


def _wait(instance: Instance, timeout: float = 15) -> str:
    """The node id, once garage answers."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        done = _docker("exec", instance.name, "/garage", "status")
        node = re.search(r"^([0-9a-f]{16})\s", done.stdout, re.MULTILINE)
        if done.returncode == 0 and node:
            return node.group(1)
        time.sleep(0.2)
    raise GarageError(f"garage {instance.name!r} did not come up; `docker logs {instance.name}`")


def _docker(*argv: str) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", *argv], capture_output=True, text=True)


def _check(done: subprocess.CompletedProcess, what: str) -> None:
    if done.returncode:
        raise GarageError(f"{what}: {done.stderr.strip()}")


def _garage(instance: Instance, *args: str) -> str:
    done = _docker("exec", instance.name, "/garage", *args)
    _check(done, f"garage {' '.join(args)}")
    return done.stdout


def _garage_ok(instance: Instance, *args: str) -> bool:
    return _docker("exec", instance.name, "/garage", *args).returncode == 0
