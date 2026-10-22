"""Running sibling work concurrently: a dag's wave, a fan's lanes.

``run_all`` is ``gather`` with a failure policy, and the one place that policy is
decided. It starts everything it is given at once: *when* work may start is
coordination's business (waves, staged lanes), *how much may run at once* is the
substrate's. A runner waiting for a worker or a quota does so inside its own
coroutine, invisible here — and a lane still waiting when a sibling fails is simply
cancelled, having spawned nothing.

**Fail fast** is the only policy today. The first failure cancels everything still
running beside it, and cancellation reaches the substrate because ``Runner.run`` kills
its handle when interrupted — so a failed lane in a 1,000-wide ECS fan stops the other
999 tasks rather than paying for them. Ctrl-C and a cancelled parent take the same path.

**Drain** is the planned alternative (design ADR 0007): start nothing new, let what is
in flight finish, then fail with the first error. It only pays once resume skips
completed nodes via their ``$done`` markers — until then the drained work would be
thrown away by the re-run anyway. It belongs here, as a second branch of ``run_all``
selected by a policy argument; the callers' "a wave/fan stops at the first error" stays
as it is. Cancellation from outside (Ctrl-C, a parent) kills under either policy.
"""
from __future__ import annotations

import asyncio
from typing import Any, Coroutine, Iterable, TypeVar


T = TypeVar("T")


async def run_all(work: Iterable[Coroutine[Any, Any, T]]) -> list[T]:
    """Run ``work`` concurrently; results in input order. On the first failure, cancel
    the rest, wait until they have stopped, and raise that failure.

    The failure is raised as itself, not wrapped in the ``ExceptionGroup`` a
    ``TaskGroup`` produces: callers catch ``DagRunError`` / ``FanError``, and a nested
    run would otherwise wrap one group per level. If several fail at once, the others
    are attached as notes.
    """
    try:
        async with asyncio.TaskGroup() as group:
            tasks = [group.create_task(w) for w in work]
    except BaseExceptionGroup as failed:
        first, *rest = failed.exceptions
        for other in rest:
            first.add_note(f"also failed: {other!r}")
        raise first from None
    return [t.result() for t in tasks]
