"""``Executor`` — the shell around a run.

Deliberately concrete, and deliberately thin. There is no ``Executor`` ABC because
the variation this project actually has lives one layer down: local versus remote is a
*runner* choice, which is what "the recursion boundary is the distribution boundary"
means. Phase 5's items are all `Runner` implementations, not alternative executors, and
inventing a second substitution point would mean two places to ask where execution mode
gets decided. If a genuine second implementation ever appears, extracting the interface
is a small refactor and nothing calling ``execute`` would notice.

What it owns is everything that is true once per run rather than once per node:

- **the store binding** — a live store built from the deployment, which is the only
  place the substrate is read;
- **the run id** — sortable, so listing runs is chronological;
- **the run's own inputs** — staged into ``<run>/<dag>/$in/`` and handed to the root dag
  as bindings, which is the convention the ``$input`` edges resolve against;
- **the plan** — written into the run's scope, so a completed run is self-describing:
  anything rendering or auditing it later has the graph beside the data, and a remote
  dag runner (item 5.2) has somewhere to fetch its slice from;
- **the result** — where the declared outputs landed, which an exit code cannot carry.
"""
from __future__ import annotations

import secrets
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping

from cascade.deployment import Deployment
from cascade.model.runner_kinds import RunnerKind
from cascade.model.runner_overrides import RunnerOverrides
from cascade.plan.plan import Plan
from cascade.store.base import Store
from cascade.store.registry import from_config

from cascade.engine.binding import InputBinding, InputBindings
from cascade.engine.instance_path import InstancePath
from cascade.engine.run_spec import RunSpec
from cascade.engine.runner.registry import RunnerEnv
from cascade.engine.runner.runner_dag import DagRunner


RUN_INPUTS = "$in"
PLAN_KEY = "plan"


class ExecutorError(Exception):
    """A run could not be started."""


def new_run_id() -> str:
    """Sortable, so listing a bucket lists runs in order; short random suffix so two
    runs starting in the same second do not collide."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"{stamp}-{secrets.token_hex(3)}"


@dataclass
class RunResult:
    """Where a finished run put things. An exit code cannot say this, and the CLI, a
    UI, and any downstream step all need it."""

    run_id: str
    entrypoint: str
    outputs: dict[str, tuple[tuple[str, ...], str]] = field(default_factory=dict)
    store: Store | None = None

    def fetch(self, port: str) -> Any:
        """The value of one declared output. Resolves the alias, so the caller never
        needs to know which inner node actually produced it."""
        if port not in self.outputs:
            raise ExecutorError(f"run has no output port {port!r}")
        if self.store is None:
            raise ExecutorError("no store attached to this result")
        scope, key = self.outputs[port]
        return self.store.get_json(key, at=scope)


def _live(config) -> Store:
    _kind, store_cls, _config_cls = from_config(config)
    return store_cls(config)


class Executor:
    def __init__(
        self,
        plan: Plan,
        deployment: Deployment | None = None,
        store: Store | None = None,
        env: RunnerEnv | None = None,
        overrides: Mapping[RunnerKind, RunnerOverrides] | None = None,
    ):
        """``deployment`` supplies the store and the per-kind runner defaults; ``store``
        overrides it directly, which is what tests and ``--backend`` flags want."""
        if deployment is None and store is None:
            raise ExecutorError("an executor needs either a deployment or a store")
        self.plan = plan
        self.deployment = deployment
        self.env = env or RunnerEnv()
        self.overrides = dict(
            overrides if overrides is not None
            else (deployment.runners if deployment else {})
        )
        self._base = store if store is not None else _live(deployment.store)

    # ------------------------------------------------------------------ inputs
    def _declared_inputs(self) -> dict[str, Any]:
        signature = self.plan.signatures.get(self.plan.entrypoint)
        if signature is None:
            raise ExecutorError(f"no signature for entrypoint {self.plan.entrypoint!r}")
        return dict(signature.inputs)

    def _stage_inputs(self, dag_store: Store, inputs: Mapping[str, Any]) -> InputBindings:
        """Write the run's own inputs into ``$in/`` and bind them there.

        This is the one place the ``$input`` convention is defined; every ``$input``
        edge in the entrypoint dag resolves against it.
        """
        declared = self._declared_inputs()
        missing = set(declared) - set(inputs)
        unexpected = set(inputs) - set(declared)
        if missing:
            raise ExecutorError(
                f"missing input(s) for {self.plan.entrypoint!r}: {sorted(missing)}"
            )
        if unexpected:
            raise ExecutorError(
                f"unknown input(s) for {self.plan.entrypoint!r}: {sorted(unexpected)}"
            )
        for port, value in inputs.items():
            dag_store.put_json(port, value, at=(RUN_INPUTS,))
        return InputBindings(
            inputs=tuple(
                InputBinding(port=port, scope=(RUN_INPUTS,), key=port) for port in inputs
            )
        )

    # --------------------------------------------------------------------- run
    async def run(
        self,
        inputs: Mapping[str, Any] | None = None,
        run_id: str | None = None,
    ) -> RunResult:
        run_id = run_id or new_run_id()
        dag = self.plan.entrypoint
        path = InstancePath.root(run_id).child(dag)

        run_store = _live(self._base.config.subscope((run_id,)))
        dag_store = _live(self._base.config.subscope(path.scope))

        # a completed run is self-describing: the graph sits beside the data
        run_store.put_json(PLAN_KEY, self.plan.encode())

        bindings = self._stage_inputs(dag_store, inputs or {})

        runner = DagRunner(dag, self.plan, self.env, self.overrides)
        code = await runner.run(
            RunSpec(
                name=dag,
                run_id=run_id,
                instance_id=str(path),
                store_out=dag_store,
                inputs=bindings,
            )
        )
        if code != 0:
            raise ExecutorError(f"run {run_id} exited {code}")

        return RunResult(
            run_id=run_id,
            entrypoint=dag,
            outputs=runner.output_scopes(),
            store=dag_store,
        )


async def execute(
    plan: Plan,
    deployment: Deployment | None = None,
    inputs: Mapping[str, Any] | None = None,
    *,
    store: Store | None = None,
    env: RunnerEnv | None = None,
    run_id: str | None = None,
) -> RunResult:
    """One-shot convenience over ``Executor``."""
    return await Executor(plan, deployment=deployment, store=store, env=env).run(
        inputs=inputs, run_id=run_id
    )