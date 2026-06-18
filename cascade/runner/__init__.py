"""The runner package: launch one node instance and track it to completion.

Split into focused modules — base (the spawn/state abstraction), subprocess
(local docker), ecs_task (Fargate), subdag_local (in-process builtins like the
collector), echo, hooked, and registry — but re-exported here so the rest of the
package keeps importing ``from cascade.runner import X`` unchanged.

The container entrypoint env contract (what images implement):

    CASCADE_RUN_ID        the run id
    CASCADE_NODE_ID       this node's id
    CASCADE_INSTANCE_KEY  scatter instance key ("_root" if not scattered)
    CASCADE_INPUT_KEYS    JSON: { binding_name: store_key, ... }
    CASCADE_OUTPUT_PREFIX store key prefix to write output(s) under
    CASCADE_MANIFEST_KEY  store key to write the result metadata blob to
    CASCADE_ARGS          JSON: the node's static args (kwargs)
    CASCADE_PORTS         JSON: per-port encoding/mapping plan
    CASCADE_STORE_CONF    JSON: the store config (S3 only; local uses the mount)
"""

from .base import (
    RunSpec, TaskStatus, Handle, Runner, SimpleRunner, RunnerError, _TaskHandle,
)
from .subprocess import SubprocessRunner, _SubprocessHandle
from .ecs_task import EcsTaskRunner, _EcsHandle
from .subdag_local import BuiltinRunner, builtin, _BUILTINS, _collect
from .echo import EchoRunner
from .hooked import HookedRunner, _ext_for
from .registry import RunnerRegistry, check_deployment_satisfies

__all__ = [
    "RunSpec", "TaskStatus", "Handle", "Runner", "SimpleRunner", "RunnerError",
    "SubprocessRunner", "EcsTaskRunner", "BuiltinRunner", "builtin",
    "EchoRunner", "HookedRunner", "RunnerRegistry", "check_deployment_satisfies",
]
