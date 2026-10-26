"""Bridge networks with a fixed subnet and gateway."""
from __future__ import annotations

from dataclasses import dataclass

from cascade.docker.cli import docker, docker_json


class NetworkError(Exception):
    """A network cannot be had as asked: one of that name differs, or the subnet is taken."""


@dataclass(frozen=True)
class Network:
    name: str
    subnet: str
    gateway: str
    """The host's own address on the bridge: what a container reaches the host at."""


async def ensure_network(network: Network) -> None:
    """Create ``network`` unless a network of that name exists with the same subnet and
    gateway. One that differs is an error, never reused or replaced: other containers may
    be attached to it."""
    raw = await docker_json("network", "inspect", network.name)
    if raw is not None:
        configs = (raw[0].get("IPAM") or {}).get("Config") or []
        found = [(c.get("Subnet"), c.get("Gateway")) for c in configs]
        if found != [(network.subnet, network.gateway)]:
            raise NetworkError(
                f"docker network {network.name!r} exists with subnet/gateway {found}, not "
                f"{network.subnet} / {network.gateway}; remove it (`docker network rm "
                f"{network.name}`) and it is created again"
            )
        return
    code, _, err = await docker(
        "network", "create", "--driver", "bridge",
        "--subnet", network.subnet, "--gateway", network.gateway, network.name,
    )
    if code == 0:
        return
    message = err.decode(errors="replace").strip()
    if "already exists" in message:  # created meanwhile, by another run: check it
        return await ensure_network(network)
    if "overlaps" in message:
        raise NetworkError(
            f"cannot create docker network {network.name!r}: subnet {network.subnet} is "
            "taken by another docker network (`docker network ls`, then `docker network "
            "inspect`); remove that network or free the subnet"
        )
    raise NetworkError(f"cannot create docker network {network.name!r}: {message}")
