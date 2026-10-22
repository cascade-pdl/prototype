"""The shell around a run: turn a plan and a deployment into an executed pipeline.

Above ``engine`` deliberately. The executor is the only component that holds a
``Deployment``, so putting it inside ``engine`` made coordination depend on configuration —
the last upward import in the package graph, and the one thing ``test_layering`` had to
list as a known exception.

Its job is the translation: a deployment becomes a live store and a configured ref builder,
and the engine receives neither a deployment nor a substrate, only the pieces.
"""
from cascade.executor.executor import (
    PLAN_KEY,
    RUN_INPUTS,
    Executor,
    ExecutorError,
    RunResult,
    execute,
    new_run_id,
)

__all__ = [
    "PLAN_KEY",
    "RUN_INPUTS",
    "Executor",
    "ExecutorError",
    "RunResult",
    "execute",
    "new_run_id",
]
