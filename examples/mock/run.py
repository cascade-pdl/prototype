"""Run a mock pipeline as local subprocesses against a local file store.

Stands in for the Executor (item 3.1), which will do this from a project's
cascade.toml + deployment.yaml instead of hard-coded values.
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

from yaml import safe_load

from cascade.model.pipeline import Pipeline
from cascade.plan.compile import compile_pipeline
from cascade.protocol.binding import InputBindings
from cascade.engine.run_spec import RunSpec
from cascade.engine.runner.runner_dag import DagRunner
from cascade.store.file_store import FileConfig, FileStore


async def run(pipeline_path: str, root: str, run_id: str = "r1") -> int:
    plan = compile_pipeline(Pipeline.decode(safe_load(Path(pipeline_path).read_text())))
    dag = plan.entrypoint

    # what the deployment supplies: a backend and a base scope
    base = FileConfig(root=root, scope=("mock",))
    store = FileStore(base.subscope((run_id, dag)))

    runner = DagRunner(dag, plan)
    code = await runner.run(
        RunSpec(
            name=dag,
            run_id=run_id,
            instance_id=f"{run_id}/{dag}",
            store_out=store,
            inputs=InputBindings(),
        )
    )
    print(f"\nexit: {code}")
    print("declared outputs ->", runner.output_scopes())
    for port, (scope, key) in runner.output_scopes().items():
        payload = store.read_json(key, at=scope)
        n = len(payload) if isinstance(payload, list) else 1
        print(f"  {port}: {n} item(s) at {'/'.join(scope)}/{key}")
        if isinstance(payload, list):
            print(f"    first: {json.dumps(payload[0])}")
    return code


if __name__ == "__main__":
    pipeline = sys.argv[1] if len(sys.argv) > 1 else "examples/mock/pipeline_flat.yaml"
    root = sys.argv[2] if len(sys.argv) > 2 else "_mockstore"
    raise SystemExit(asyncio.run(run(pipeline, root)))