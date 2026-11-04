import asyncio
import inspect
import traceback
from unittest.mock import AsyncMock

import pytest

from chancy.utils import TaskManager

pytestmark = pytest.mark.asyncio


async def started():
    await asyncio.sleep(0)
    await asyncio.sleep(0)


async def test_cancel_all_preserves_async_cleanup():
    manager = TaskManager()
    cleaning = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()

    async def slow():
        try:
            await asyncio.Future()
        finally:
            cleaning.set()
            await release.wait()
            finished.set()

    async def fast():
        try:
            await asyncio.Future()
        finally:
            await cleaning.wait()

    slow_task = manager.add("slow", slow())
    manager.add("fast", fast())
    await started()
    first = asyncio.create_task(manager.cancel_all())
    await cleaning.wait()
    second = asyncio.create_task(manager.cancel_all())
    await started()
    assert slow_task.cancelling() == 1
    assert not first.done() and not second.done()
    release.set()
    await asyncio.gather(first, second)
    assert finished.is_set()
    assert not manager


async def test_issue_105_done_callbacks_are_idempotent():
    manager = TaskManager()
    loop = asyncio.get_running_loop()
    errors = []
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda loop, context: errors.append(context))

    async def slow(other):
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            while not other.done():
                await asyncio.sleep(0)
            await asyncio.sleep(0)
            raise

    try:
        fast = manager.add("fast", asyncio.sleep(3600))
        manager.add("slow", slow(fast))
        await asyncio.sleep(0)
        await manager.cancel_all()
        await asyncio.sleep(0)
        assert not errors
    finally:
        loop.set_exception_handler(previous)


async def test_all_waiters_observe_failures_after_cancellation():
    manager = TaskManager()
    failures = [ValueError("first"), RuntimeError("second")]
    for failure in failures:
        manager.add("duplicate label", AsyncMock(side_effect=failure)())
    await started()
    assert not manager  # Only live tasks count.
    await manager.cancel_all()

    async def observe():
        with pytest.raises(ExceptionGroup) as caught:
            await manager.wait_for_shutdown()
        assert set(caught.value.exceptions) == set(failures)

    await asyncio.gather(observe(), observe())
    await observe()


async def test_failures_during_cancel_are_retained():
    manager = TaskManager()
    failure = ValueError("cleanup failed")

    async def service():
        try:
            await asyncio.Future()
        finally:
            raise failure

    manager.add("service", service())
    await started()
    await manager.cancel_all()
    for _ in range(2):
        with pytest.raises(ValueError) as caught:
            await manager.wait_for_shutdown()
        assert caught.value is failure


async def test_cancellation_uses_handles_and_closes_admission():
    manager = TaskManager()
    failed = manager.add("same", AsyncMock(side_effect=ValueError("failed"))())
    live = manager.add("same", asyncio.sleep(3600))
    await started()
    await manager.cancel(failed)
    assert not live.done()
    with pytest.raises(TypeError, match="handle"):
        await manager.cancel("same")
    await manager.cancel(live)
    assert live.cancelled()
    await manager.cancel_all()
    coroutine = asyncio.sleep(0)
    with pytest.raises(RuntimeError, match="closed"):
        manager.add("late", coroutine)
    assert inspect.getcoroutinestate(coroutine) == inspect.CORO_CLOSED


async def test_cancel_timeout_does_not_cancel_cleanup_again():
    manager = TaskManager()
    release = asyncio.Event()

    async def service():
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            await release.wait()

    task = manager.add("stubborn", service())
    await started()
    try:
        for _ in range(2):
            with pytest.raises(TimeoutError, match="stubborn"):
                await manager.cancel_all(timeout=0.01)
        assert task.cancelling() == 1
        assert task in manager
    finally:
        release.set()
        await manager.cancel_all()


async def test_repeated_waiters_preserve_original_traceback():
    manager = TaskManager()

    async def fail():
        raise ValueError("original failure")

    manager.add("failed", fail())
    await started()
    traces = []
    for _ in range(3):
        with pytest.raises(ValueError) as caught:
            await manager.wait_for_shutdown()
        traces.append(
            [
                frame.name
                for frame in traceback.extract_tb(caught.value.__traceback__)
            ]
        )
    assert traces[0] == traces[1] == traces[2]
    assert traces[0].count("fail") == 1
