"""ECS task runner: launches a node as a Fargate task and polls describe_tasks.

The real spawn/poll separation lives here: spawn = run_task -> task ARN handle;
the handle's state() = describe_tasks(arn). boto3 is lazy and the calls run in a
thread so they don't block the event loop. Untested against real ECS yet.
"""

from __future__ import annotations

import asyncio

from .base import Runner, Handle, TaskStatus, RunSpec


class EcsTaskRunner(Runner):
    """Launches the node as an ECS task and polls until it stops.

    Constructed with **deployment** config (the ECS cluster, region, role,
    networking — supplied at run time, not from the pipeline). Each ``run`` reads
    the node's **per-node** config (cpu/memory) from ``spec.runner_config``. So
    one EcsTaskRunner instance serves all ECS nodes; each task gets its own
    resources from its ref.

    The boto3 calls are sketched but guarded — wire them up and test against real
    ECS. This keeps the structure correct and the dependency (boto3) lazy.
    """

    def __init__(self, deployment, poll_interval: float = 5.0, project_name: str | None = None):
        # deployment: cascade.runners_config.EcsDeployment
        self.deployment = deployment
        self.project_name = project_name
        self.sleep_time = poll_interval   # base Runner.run polls state() at this rate

    def spawn(self, spec: RunSpec) -> Handle:  # pragma: no cover - needs real ECS
        """Start the ECS task (run_task) and return a handle holding the task
        ARN. This is the real spawn/poll separation: spawn returns fast with a
        detached reference (the ARN); the handle's state() polls describe_tasks.
        The ARN handle could be persisted and polled from elsewhere."""
        return _EcsHandle(self.deployment, spec, self.project_name)


class _EcsHandle(Handle):  # pragma: no cover - needs real ECS
    """Holds an ECS task ARN (started lazily on first state()) and polls
    describe_tasks for its status. The boto3 calls run in a thread so they don't
    block the event loop; the poll cadence is driven by the base Runner.run."""

    def __init__(self, deployment, spec: RunSpec, project_name: str | None = None):
        self.deployment = deployment
        self.spec = spec
        self.project_name = project_name
        self._arn = None

    def _client(self):
        import boto3
        return boto3.client("ecs", region_name=self.deployment.region)

    async def _start(self):
        from .. import naming
        cfg = self.spec.runner_config
        cpu = getattr(cfg, "cpu", None)
        memory = getattr(cfg, "memory", None)
        ref = self.spec.ref_name or self.spec.node_id
        # the override's container name MUST match the taskdef's container name,
        # which provisioning set to the ref name (naming.container_name)
        cname = naming.container_name(ref)
        env_overrides = [{"name": k, "value": v} for k, v in self.spec.env.items()]
        container_override = {"name": cname, "environment": env_overrides}
        if cpu:
            container_override["cpu"] = cpu
        if memory:
            container_override["memory"] = memory
        net = {
            "awsvpcConfiguration": {
                "subnets": self.deployment.subnets,
                "securityGroups": self.deployment.security_groups,
                "assignPublicIp": "ENABLED",
            }
        } if self.deployment.subnets else {}

        # the conventional taskdef family provisioning registered for this ref
        # (naming is the single shared source of truth, so this family name
        # matches what `cascade provisioning ecs-task gen-taskdef` produced)
        task_def = None
        if self.project_name:
            task_def = naming.taskdef_family(self.project_name, ref)

        def _run_task():
            kwargs = dict(
                cluster=self.deployment.cluster,
                launchType="FARGATE",
                overrides={"containerOverrides": [container_override]},
                networkConfiguration=net,
            )
            if task_def:
                kwargs["taskDefinition"] = task_def
            return self._client().run_task(**kwargs)
        resp = await asyncio.to_thread(_run_task)
        self._arn = resp["tasks"][0]["taskArn"]

    async def state(self) -> TaskStatus:
        if self._arn is None:
            await self._start()
        def _describe():
            return self._client().describe_tasks(
                cluster=self.deployment.cluster, tasks=[self._arn])
        desc = await asyncio.to_thread(_describe)
        task = desc["tasks"][0]
        if task["lastStatus"] == "STOPPED":
            containers = task.get("containers", [])
            exit_code = containers[0].get("exitCode", 1) if containers else 1
            return TaskStatus(running=False, exit_code=int(exit_code))
        return TaskStatus(running=True)

