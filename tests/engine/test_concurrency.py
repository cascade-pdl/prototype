"""``run_all``: concurrent sibling work, fail fast (``engine/concurrency.py``)."""
import asyncio

import pytest

from cascade.engine.concurrency import run_all


@pytest.mark.asyncio
async def test_results_come_back_in_input_order():
    async def after(delay, value):
        await asyncio.sleep(delay)
        return value

    assert await run_all([after(0.03, "a"), after(0.0, "b"), after(0.01, "c")]) == ["a", "b", "c"]


@pytest.mark.asyncio
async def test_the_first_failure_cancels_the_rest_and_is_raised_unwrapped():
    cancelled = []

    async def slow(name):
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.append(name)
            raise

    async def boom():
        await asyncio.sleep(0.01)
        raise ValueError("lane 2 failed")

    with pytest.raises(ValueError, match="lane 2 failed"):  # not an ExceptionGroup
        await asyncio.wait_for(run_all([slow("a"), boom(), slow("b")]), timeout=5)
    assert sorted(cancelled) == ["a", "b"]


@pytest.mark.asyncio
async def test_simultaneous_failures_are_reported_as_notes():
    async def boom(msg):
        raise RuntimeError(msg)

    with pytest.raises(RuntimeError) as info:
        await run_all([boom("first"), boom("second")])
    assert str(info.value) == "first"
    assert any("second" in note for note in info.value.__notes__)


@pytest.mark.asyncio
async def test_cancellation_from_outside_reaches_every_child():
    cancelled = []

    async def slow(name):
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.append(name)
            raise

    outer = asyncio.create_task(run_all([slow("a"), slow("b")]))
    await asyncio.sleep(0.02)
    outer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await outer
    assert sorted(cancelled) == ["a", "b"]
