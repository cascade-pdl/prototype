"""Runners: how a unit of work is started and awaited.

A ``Runner`` implements one atom, ``spawn``, returning a ``Handle``; the
spawn-then-poll loop is written once in ``Runner.run``. Kinds differ only in what
they spawn — a container, a subprocess, or an in-process coroutine — which is what
lets a dag or a fanned node be substituted for a ref without the caller caring.
"""
from cascade.engine.runner.runner import Runner
from cascade.engine.runner.handle import Handle
from cascade.engine.runner.run_status import RunStatus
from cascade.engine.runner.runner_coro import (
    HandleCoro,
    RunnerCoroBase,
    RunnerCoro,
    RunnerAwaitable,
)
from cascade.engine.runner.runner_echo import RunnerEcho
from cascade.engine.runner.runner_subprocess import RunnerSubprocess, HandleSubprocess
from cascade.engine.runner.runner_docker import RunnerDocker, HandleDocker
from cascade.engine.runner.registry import RunnerEnv, build_runner, merge_overrides
from cascade.engine.binding import InputBinding, InputBindings
from cascade.engine.instance_path import InstancePath
from cascade.engine.runner.runner_dag import DagRunner, DagRunError

__all__ = [
    "Runner",
    "Handle",
    "RunStatus",
    "HandleCoro",
    "RunnerCoroBase",
    "RunnerCoro",
    "RunnerAwaitable",
    "RunnerEcho",
    "RunnerSubprocess",
    "HandleSubprocess",
    "RunnerDocker",
    "HandleDocker",
    "RunnerEnv",
    "build_runner",
    "merge_overrides",
    "InputBinding",
    "InputBindings",
    "InstancePath",
    "DagRunner",
    "DagRunError",
]