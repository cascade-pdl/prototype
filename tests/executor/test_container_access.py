"""What a run sets up for local docker containers, and for exactly how long: the network
whenever one runs, the credentials endpoint only when the store is S3 too."""
import pytest
from yaml import safe_load

import cascade.executor.executor as executor_module
from cascade.docker.images import ImageConfig
from cascade.executor import Executor
from cascade.model.pipeline import Pipeline
from cascade.plan.compile import compile_pipeline
from cascade.runners.credentials import ContainerCredentials
from cascade.runners.docker import CONTAINER_NETWORK
from cascade.store.file_store import FileConfig, FileStore
from cascade.store.s3_store import S3Config, S3Store

PIPELINE = """
entrypoint: main
input: []
refs:
  - name: tool
    runner: %s
    config: %s
    resources: { cpu: 0.5, memory: 256MiB }
    input: []
    output: [ { name: out, type: string } ]
dags:
  - name: main
    input: []
    nodes: [ { name: t, runs: tool } ]
    output: [ { node: t, field: out, as: out } ]
"""
DOCKER = PIPELINE % ("docker", "{ image: 'tool:1', wrap: false }")
SUBPROCESS = PIPELINE % ("subprocess", "{ cmd: ['true'], wrap: false }")
SERVED = ContainerCredentials(uri="http://169.254.170.2:1/credentials", token="t")


class Stop(Exception):
    """Ends the run once the builder is configured: nothing past that is under test."""


@pytest.fixture
def seen(monkeypatch):
    seen = {"network": [], "endpoint": [], "credentials": "unset"}

    async def nothing(*a, **k):
        return []

    async def resolve(plan, placements, backends):
        return {p.ref: ImageConfig("sha256:abc", (), ()) for p in placements
                if plan.run_config[p.ref].runner.value == "docker"}

    async def ensure(network):
        seen["network"].append(network)

    class Endpoint:
        def __init__(self, host):
            seen["endpoint"].append(("made", host))

        def __enter__(self):
            seen["endpoint"].append("open")
            return SERVED

        def __exit__(self, *exc):
            seen["endpoint"].append("closed")

    def configure(**kwargs):
        seen["credentials"] = kwargs["credentials"]
        seen["endpoint"].append("builder")
        raise Stop

    async def usable(self):
        pass

    # these runs are about the network and the endpoint; the store is never reached
    monkeypatch.setattr(S3Store, "check", usable)
    monkeypatch.setattr(executor_module, "prepare_images", nothing)
    monkeypatch.setattr(executor_module, "resolve_images", resolve)
    monkeypatch.setattr(executor_module, "ensure_network", ensure)
    monkeypatch.setattr(executor_module, "CredentialsEndpoint", Endpoint)
    monkeypatch.setattr(executor_module, "configure_ref_builder", configure)
    return seen


async def _run(pipeline, store):
    plan = compile_pipeline(Pipeline.decode(safe_load(pipeline)))
    with pytest.raises(Stop):
        await Executor(plan, store=store).run(run_id="r1")


S3 = S3Store(S3Config(bucket="b", endpoint="http://localhost:3900"))


@pytest.mark.asyncio
async def test_docker_on_s3_gets_the_network_and_an_endpoint_open_for_the_run(seen):
    await _run(DOCKER, S3)
    assert seen["network"] == [CONTAINER_NETWORK]
    assert seen["endpoint"] == [("made", "169.254.170.2"), "open", "builder", "closed"]
    assert seen["credentials"] == SERVED


@pytest.mark.asyncio
async def test_docker_on_a_file_store_gets_the_network_and_no_credentials(seen, tmp_path):
    await _run(DOCKER, FileStore(FileConfig(root=str(tmp_path))))
    assert seen["network"] == [CONTAINER_NETWORK]
    assert seen["endpoint"] == ["builder"]
    assert seen["credentials"] is None


@pytest.mark.asyncio
async def test_no_container_no_network_no_endpoint(seen):
    """A subprocess ref has the host's own identity already."""
    await _run(SUBPROCESS, S3)
    assert seen["network"] == []
    assert seen["endpoint"] == ["builder"]
    assert seen["credentials"] is None
