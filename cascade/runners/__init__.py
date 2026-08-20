"""Substrate runners: the ways a *ref* can actually be executed.

One package for every "somewhere the work happens" — this process, a subprocess, a local
container, and later a remote scheduler. They are peers: local docker is a substrate in the
same sense ECS is, which is why its own configuration (credentials, user mapping) belongs
here rather than in a general-purpose context object.

**Coordination runners are deliberately elsewhere.** ``DagRunner`` and ``FanRunner`` live in
``cascade.engine`` because they spawn nothing — they drive *other* runners. The old
``runner_`` prefix put all six in one folder and made them read as peers, which is how the
registry came to look like it should build all of them. It should only ever build refs:
``RunnerKind`` has no ``dag`` member, and a dag runner is constructed from a plan, not from
a ref's config.
"""
from cascade.runners.coro import RunnerAwaitable, RunnerCoro
from cascade.runners.docker import HandleDocker, RunnerDocker
from cascade.runners.echo import RunnerEcho
from cascade.runners.process import HandleSubprocess, RunnerSubprocess
from cascade.runners.registry import (
    RunnerEnv,
    RunnerKindError,
    build_runner,
    merge_overrides,
)

__all__ = [
    "HandleDocker",
    "HandleSubprocess",
    "RunnerAwaitable",
    "RunnerCoro",
    "RunnerDocker",
    "RunnerEcho",
    "RunnerEnv",
    "RunnerKindError",
    "RunnerSubprocess",
    "build_runner",
    "merge_overrides",
]
