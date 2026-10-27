"""Concurrency limits end to end: real processes, a fan, a backend's and a pipeline's caps.

Each lane records when its process started and ended; the peak overlap of those
intervals is how many really ran at once.
"""
import os
import sys
from pathlib import Path

import pytest
from yaml import safe_load

from cascade.deployment import Deployment
from cascade.executor import Executor
from cascade.model.pipeline import Pipeline
from cascade.plan.compile import compile_pipeline


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "pyproject.toml").is_file())

LANE = """
import os, sys, time
import cascade.node as cn

with cn.from_env(os.environ) as n:
    start = time.monotonic()
    time.sleep(0.3)
    log = os.path.join(n.args["log"], str(n.read("number")))
    with open(log, "w") as f:
        f.write(f"{start} {time.monotonic()}")
    n.write("done", n.read("number"))
"""

PIPELINE = """
entrypoint: main
limits: %LIMITS%
input: [ { name: numbers, type: "int[]" } ]
refs:
  - name: work
    runner: subprocess
    resources: { cpu: 1, memory: 256MiB }
    %LIMIT%
    config: { cmd: [%PYTHON%, %SCRIPT%], wrap: false }
    input:  [ { name: number, type: int } ]
    output: [ { name: done, type: int } ]
dags:
  - name: main
    input: [ { name: numbers, type: "int[]" } ]
    nodes:
      - name: lanes
        runs: work
        scatter: number
        args: { log: %LOG% }
        depends_on: [ { node: "$input", field: numbers, as: number } ]
    output: [ { node: lanes, field: done, as: done } ]
"""


@pytest.fixture(autouse=True)
def _dev_tree_importable(monkeypatch):
    existing = os.environ.get("PYTHONPATH", "")
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join(filter(None, [str(ROOT), existing])))


def _peak(log: Path) -> int:
    events = []
    for f in log.iterdir():
        start, end = map(float, f.read_text().split())
        events += [(start, 1), (end, -1)]
    live = peak = 0
    for _, step in sorted(events):  # an end sorts before a start at the same instant
        live += step
        peak = max(peak, live)
    return peak


@pytest.mark.asyncio
@pytest.mark.parametrize("limit, limits, expected", [
    ("", {}, 2),                         # the backend's concurrency
    ("limit: heavy", {"heavy": 1}, 1),   # a named limit below it
])
async def test_a_fan_runs_no_wider_than_its_limits(tmp_path, limit, limits, expected):
    log = tmp_path / "log"
    log.mkdir()
    script = tmp_path / "lane.py"
    script.write_text(LANE)
    text = (PIPELINE.replace("%LIMITS%", repr(limits)).replace("%LIMIT%", limit).replace("%PYTHON%", repr(sys.executable))
            .replace("%SCRIPT%", repr(str(script))).replace("%LOG%", repr(str(log))))
    plan = compile_pipeline(Pipeline.decode(safe_load(text)))
    deployment = Deployment.decode({
        "store": {"kind": "file", "config": {"root": str(tmp_path / "store")}},
        "backends": [{"name": "local", "kind": "local", "max": {"cpu": 4, "memory": "8GiB"},
                      "concurrency": 2}],
    })

    result = await Executor(plan, deployment=deployment, cwd=str(tmp_path)).run(
        inputs={"numbers": [1, 2, 3, 4, 5]}
    )

    assert await result.fetch("done") == [1, 2, 3, 4, 5]
    assert _peak(log) == expected
