"""``Executor`` — the shell around a run.

Deliberately concrete, and deliberately thin. There is no ``Executor`` ABC because
the variation this project actually has lives one layer down: local versus remote is a
*runner* choice, which is what "the recursion boundary is the distribution boundary"
means. Remote execution (ECS, a dag in a container) is all `Runner` implementations, not
alternative executors, and inventing a second substitution point would mean two places to ask where execution mode
gets decided. If a genuine second implementation ever appears, extracting the interface
is a small refactor and nothing calling ``execute`` would notice.

What it owns is everything that is true once per run rather than once per node:

- **the store binding** — a live store built from the deployment, which is the only
  place the substrate is read;
- **the run id** — sortable, so listing runs is chronological;
- **the run's own inputs** — staged into ``<run>/<dag>/$in/`` and handed to the root dag
  as bindings, which is the convention the ``$input`` edges resolve against;
- **the spec budget** — every launch the run would make is checked to fit it before
  anything is written or started, so a node too big for ECS is refused up front;
- **the placement** — which backend each ref runs on, decided once from the plan and
  the deployment before anything starts, and bound into the ref builder;
- **container access** — when a ref runs in a local docker container: the network it
  joins, and, with an S3 store, the endpoint its containers fetch credentials from, served
  for exactly the run's lifetime;
- **the limits** — the backends' ``concurrency`` and the plan's named ``limits``,
  counted across the whole run by the ref builder made for it;
- **the plan** — written into the run's scope, so a completed run is self-describing:
  anything rendering or auditing it later has the graph beside the data, and a remote
  dag runner has somewhere to fetch its slice from. The placement is written
  beside it: where each ref actually ran, even if the deployment file changes later;
- **the result** — where the declared outputs landed, which an exit code cannot carry.
"""
from __future__ import annotations

import asyncio
import secrets
from contextlib import ExitStack
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any, Mapping

from cascade.deployment import Deployment
from cascade.docker.images import ImageConfig
from cascade.docker.network import NetworkError, ensure_network
from cascade.plan.plan import Plan
from cascade.store.base import Store, StoreError
from cascade.store.registry import from_config
from cascade.store.s3_store import S3Config

from cascade.protocol.binding import INPUTS_SCOPE, InputBinding, InputBindings
from cascade.protocol.instance_path import InstancePath
from cascade.protocol.run_spec import RunSpec, SpecTooLarge, check_launch
from cascade.protocol.runner import Runner
from cascade.runners.build import RefRunnerBuilder, configure_ref_builder
from cascade.runners.credentials import (
    ContainerCredentials,
    CredentialsEndpoint,
    CredentialsError,
)
from cascade.runners.docker import CONTAINER_NETWORK
from cascade.runners.images import ImageBuildError, prepare_images, resolve_images
from cascade.engine.dag import DagRunError, DagRunner
from cascade.engine.fan import FanError
from cascade.executor.placement import place
from cascade.substrate.backends import uncapped_local
from cascade.substrate.placement import PlacementError


RUN_INPUTS = INPUTS_SCOPE
PLAN_KEY = "plan"
PLACEMENT_KEY = "placement"
IMAGES_KEY = "images"
DEPLOYMENT_KEY = "deployment"


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

    async def fetch(self, port: str) -> Any:
        """The value of one declared output. Resolves the alias, so the caller never
        needs to know which inner node actually produced it."""
        if port not in self.outputs:
            raise ExecutorError(f"run has no output port {port!r}")
        if self.store is None:
            raise ExecutorError("no store attached to this result")
        scope, key = self.outputs[port]
        # read, not get_json: a gathered port is a collection descriptor, and the caller
        # should not have to know whether it was materialised or left in place
        return await self.store.read_json(key, at=scope)


def _live(config) -> Store:
    _kind, store_cls, _config_cls = from_config(config)
    return store_cls(config)


class Executor:
    def __init__(
        self,
        plan: Plan,
        deployment: Deployment | None = None,
        store: Store | None = None,
        build_ref: RefRunnerBuilder | None = None,
        cwd: str | None = None,
    ):
        """``deployment`` supplies the store and the backends; ``store`` overrides the
        store directly, which is what tests and ``--backend`` flags want. With no
        deployment every ref runs on the uncapped local host.

        Placement happens here, so a run that cannot be placed fails before it starts.

        ``build_ref`` is how a ref becomes a runner. Supplied here rather than assembled
        inside ``DagRunner`` so that coordination never touches configuration: the executor
        is the layer that holds a deployment, so it is the layer that turns one into
        construction.
        """
        if deployment is None and store is None:
            raise ExecutorError("an executor needs either a deployment or a store")
        self.plan = plan
        self.deployment = deployment
        self.cwd = cwd
        self._backends = deployment.backends if deployment else [uncapped_local()]
        self._base = store if store is not None else _live(deployment.store)
        try:
            self.placements = place(plan, deployment)
        except PlacementError as e:
            raise ExecutorError(
                "cannot place this run:\n" + "\n".join(f"  - {m}" for m in e.errors)
            )
        # the executor is where a deployment becomes the pieces the engine needs: a live
        # store, and a builder bound to the placement and the backends it names. The
        # builder is made per run (``_builder``), once the run's images are resolved.
        self._build_ref = build_ref

    # ------------------------------------------------------------------ inputs
    def _declared_inputs(self) -> dict[str, Any]:
        signature = self.plan.signatures.get(self.plan.entrypoint)
        if signature is None:
            raise ExecutorError(f"no signature for entrypoint {self.plan.entrypoint!r}")
        return dict(signature.inputs)

    def _bind_inputs(self, inputs: Mapping[str, Any]) -> InputBindings:
        """The run's own inputs bound at ``$in/<port>``, where ``_stage_inputs`` writes
        them.

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
        # the entrypoint's own signature is right here, so a run input is bound with its
        # declared type and config rather than a placeholder
        return InputBindings(
            inputs=tuple(
                InputBinding(
                    port=port,
                    scope=(RUN_INPUTS,),
                    key=port,
                    type=declared[port].type,
                    config=declared[port].config,
                )
                for port in inputs
            )
        )

    @staticmethod
    async def _stage_inputs(dag_store: Store, inputs: Mapping[str, Any]) -> None:
        await asyncio.gather(*(
            dag_store.put_json(port, value, at=(RUN_INPUTS,)) for port, value in inputs.items()
        ))

    @staticmethod
    def _check_spec_sizes(runner: Runner, spec: RunSpec) -> None:
        """Every launch the run would make fits the spec budget, or every one that does
        not is listed."""
        errors = []
        for leaf, leaf_spec in runner.leaves(spec):
            launch = leaf.launch_for(leaf_spec)
            if launch is None:
                continue
            try:
                check_launch(
                    launch, who=f"node {leaf_spec.node_id!r} (ref {leaf_spec.name!r})"
                )
            except SpecTooLarge as e:
                errors.append(str(e))
        if errors:
            raise ExecutorError(
                "run spec too large:\n" + "\n".join(f"  - {m}" for m in errors)
            )

    # --------------------------------------------------------------------- run
    async def run(
        self,
        inputs: Mapping[str, Any] | None = None,
        run_id: str | None = None,
    ) -> RunResult:
        # the store first: no identity, no bucket or no access fails here, in one line,
        # not after images are built and the first write is attempted
        try:
            await self._base.check()
        except StoreError as e:
            raise ExecutorError(f"cannot use the store: {e}") from None
        # images are built and resolved next, once for the whole run: a Dockerfile that
        # does not build, or an image that is missing, fails before any task starts, and
        # no instance asks Docker anything before it runs
        try:
            await prepare_images(self.plan, self.placements, self._backends, self.cwd)
            images = await resolve_images(self.plan, self.placements, self._backends)
        except ImageBuildError as e:
            raise ExecutorError(str(e))
        with ExitStack() as access:
            # ``images`` holds exactly the refs placed in local docker containers
            credentials = await self._container_access(access) if images else None
            # a builder per run: it holds the run's concurrency limits
            build_ref = self._build_ref or configure_ref_builder(
                placements=self.placements, backends=self._backends, cwd=self.cwd,
                images=images, limits=self.plan.limits, credentials=credentials,
            )
            try:
                return await self._run(build_ref, images, inputs, run_id)
            except StoreError as e:
                # the check passed, so this is access lost or narrower than it looked
                # (e.g. read-only): still the environment, still one line
                raise ExecutorError(f"store: {e}") from None

    async def _record_deployment(self, run_store: Store) -> None:
        """The deployment as this run used it, beside the placement, which names its
        backends only. The store is the one the run resolved (absolute root, ``--scope``
        applied). A run with no deployment (``--backend`` flags, tests) records none:
        it had none, and the uncapped host it ran on is not a deployment a file could
        state."""
        if self.deployment is None:
            return
        used = replace(self.deployment, store=self._base.config)
        await run_store.put_json(DEPLOYMENT_KEY, used.encode())

    async def _container_access(self, access: ExitStack) -> ContainerCredentials | None:
        """The network local containers join, made if missing, and with an S3 store the
        credentials endpoint they fetch from, open until ``access`` closes."""
        try:
            await ensure_network(CONTAINER_NETWORK)
        except NetworkError as e:
            raise ExecutorError(str(e))
        if not isinstance(self._base.config, S3Config):
            return None
        try:
            return access.enter_context(CredentialsEndpoint(CONTAINER_NETWORK.gateway))
        except (CredentialsError, OSError) as e:
            raise ExecutorError(f"cannot serve credentials to containers: {e}")

    async def _run(
        self,
        build_ref: RefRunnerBuilder,
        images: Mapping[str, ImageConfig],
        inputs: Mapping[str, Any] | None,
        run_id: str | None,
    ) -> RunResult:
        run_id = run_id or new_run_id()
        dag = self.plan.entrypoint
        path = InstancePath.root(run_id).child(dag)

        run_store = _live(self._base.config.subscope((run_id,)))
        dag_store = _live(self._base.config.subscope(path.scope))

        inputs = inputs or {}
        runner = DagRunner(dag, self.plan, build_ref)
        spec = RunSpec(
            name=dag,
            run_id=run_id,
            instance_id=str(path),
            store_out=dag_store,
            inputs=self._bind_inputs(inputs),
        )
        self._check_spec_sizes(runner, spec)

        # a completed run is self-describing: the graph sits beside the data
        await asyncio.gather(
            run_store.put_json(PLAN_KEY, self.plan.encode()),
            run_store.put_json(PLACEMENT_KEY, self.placements.encode()),
            self._record_deployment(run_store),
            # which image each docker ref actually ran: a tag can move, an id cannot
            run_store.put_json(
                IMAGES_KEY, {ref: {"id": image.id} for ref, image in sorted(images.items())}
            ),
            self._stage_inputs(dag_store, inputs),
        )

        try:
            code = await runner.run(spec)
        except (DagRunError, FanError) as e:
            # a node that failed is the run's outcome, not a bug: its own stderr has
            # already said why, so the run reports which instance, in one line
            raise ExecutorError(f"run {run_id} failed: {e}") from None
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
    build_ref: RefRunnerBuilder | None = None,
    run_id: str | None = None,
) -> RunResult:
    """One-shot convenience over ``Executor``."""
    return await Executor(plan, deployment=deployment, store=store, build_ref=build_ref).run(
        inputs=inputs, run_id=run_id
    )