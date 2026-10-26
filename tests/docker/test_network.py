"""``ensure_network`` against a stand-in ``docker``: created when missing, reused only when
it matches, never replaced."""
import json
import os

import pytest

from cascade.docker.network import Network, NetworkError, ensure_network
from tests.support.processes import posix_only

pytestmark = [
    pytest.mark.skipif(posix_only, reason="a shell script stands in for docker"),
    pytest.mark.asyncio,
]

NET = Network(name="cascade", subnet="169.254.170.0/24", gateway="169.254.170.2")

# state: `inspect` holds the inspect output if the network exists; `create_err` makes
# create fail with that message; every call is appended to `calls`
FAKE_DOCKER = """#!/bin/sh
echo "$*" >> "$STATE/calls"
case "$1 $2" in
  "network inspect")
    if [ -f "$STATE/inspect" ]; then cat "$STATE/inspect"; else exit 1; fi ;;
  "network create")
    if [ -f "$STATE/create_err" ]; then cat "$STATE/create_err" >&2; exit 1; fi ;;
  *) exit 2 ;;
esac
"""


@pytest.fixture
def state(tmp_path, monkeypatch):
    bin_dir, state = tmp_path / "bin", tmp_path / "state"
    bin_dir.mkdir()
    state.mkdir()
    (bin_dir / "docker").write_text(FAKE_DOCKER)
    (bin_dir / "docker").chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("STATE", str(state))
    return state


def _exists(state, subnet, gateway):
    (state / "inspect").write_text(
        json.dumps([{"IPAM": {"Config": [{"Subnet": subnet, "Gateway": gateway}]}}])
    )


def _calls(state):
    return (state / "calls").read_text().splitlines()


async def test_a_missing_network_is_created_with_its_subnet_and_gateway(state):
    await ensure_network(NET)
    assert _calls(state)[-1] == (
        "network create --driver bridge --subnet 169.254.170.0/24 "
        "--gateway 169.254.170.2 cascade"
    )


async def test_a_matching_network_is_reused(state):
    _exists(state, NET.subnet, NET.gateway)
    await ensure_network(NET)
    assert _calls(state) == ["network inspect cascade"]


@pytest.mark.parametrize("subnet, gateway", [
    ("172.30.0.0/16", "172.30.0.1"),        # same name, another subnet
    ("169.254.170.0/24", "169.254.170.1"),  # the gateway is the address served on
])
async def test_a_differing_network_of_that_name_is_an_error_not_replaced(state, subnet, gateway):
    _exists(state, subnet, gateway)
    with pytest.raises(NetworkError, match="docker network rm cascade"):
        await ensure_network(NET)
    assert not [c for c in _calls(state) if "create" in c or " rm " in c]


async def test_a_subnet_taken_by_another_network_is_explained(state):
    (state / "create_err").write_text(
        "Error response from daemon: invalid pool request: Pool overlaps with other one "
        "on this address space"
    )
    with pytest.raises(NetworkError, match="169.254.170.0/24 is taken"):
        await ensure_network(NET)
