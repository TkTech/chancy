import asyncio
from unittest.mock import AsyncMock

import pytest
from psycopg import Notify

from chancy import Worker
from chancy.plugin import Plugin
from chancy.utils import sleep


async def assert_cancelled(task):
    try:
        done, _ = await asyncio.wait({task}, timeout=1)
        assert done, "Task continued after its cancellation was swallowed"
        assert task.cancelled()
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("wait_kind", ["sleep", "event", "plugin"])
async def test_periodic_wait_preserves_lost_cancellation(wait_kind):
    """Force the CPython wait_for race before entering a periodic wait."""
    event = asyncio.Event()

    class TestPlugin(Plugin):
        @staticmethod
        def get_identifier():
            return "test"

    plugin = TestPlugin()

    async def run():
        await asyncio.wait_for(event.wait(), timeout=10)
        if wait_kind == "sleep":
            await sleep(60)
        elif wait_kind == "event":
            await sleep(60, events=[asyncio.Event().wait()])
        else:
            await plugin.sleep(60)

    task = asyncio.create_task(run())
    # Let wait_for create its inner task and let Event.wait suspend.
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    event.set()
    task.cancel()
    await assert_cancelled(task)


@pytest.mark.asyncio
async def test_periodic_wait_after_handled_timeout():
    """A timeout that correctly clears its cancellation does not stop polling."""
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0):
            await asyncio.Future()
    assert await sleep(0)


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["queues", "heartbeat", "updates"])
async def test_worker_stop_after_sql_swallows_cancellation(
    chancy, monkeypatch, name
):
    """Do not start another polling period after a DB call loses cancellation."""
    worker = Worker(chancy, register_signal_handlers=False)
    entered = asyncio.Event()
    cleaned_up = asyncio.Event()

    async def swallow_cancellation(*args):
        entered.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            # Model the dependency's lost cancellation on every Python version.
            return []
        finally:
            cleaned_up.set()

    if name == "queues":
        monkeypatch.setattr(chancy, "get_all_queues", swallow_cancellation)
    elif name == "heartbeat":
        monkeypatch.setattr(worker, "announce_worker", swallow_cancellation)
    else:
        worker.outgoing.put_nowait(object())

        async def flush():
            if asyncio.current_task().get_name() == "updates":
                worker.outgoing.get_nowait()
                await swallow_cancellation()

        monkeypatch.setattr(worker, "flush", flush)

    task = worker.manager.add(name, getattr(worker, f"_maintain_{name}")())
    await entered.wait()
    stop = asyncio.create_task(worker.stop(timeout=0.05))
    try:
        done, _ = await asyncio.wait({stop}, timeout=1)
        assert done, "Final teardown waited indefinitely for the task"
        assert stop.result() is True
        assert task.cancelled()
        assert cleaned_up.is_set()
        async with chancy.pool.connection() as conn:
            assert conn.info.transaction_status.name == "IDLE"
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await stop


@pytest.mark.asyncio
@pytest.mark.parametrize("deliver_notification", [False, True])
async def test_notifications_preserve_lost_cancellation(
    chancy_just_app, monkeypatch, deliver_notification
):
    """Check cancellation after a notification or an empty polling period."""
    worker = Worker(chancy_just_app, register_signal_handlers=False)
    entered = asyncio.Event()
    connection = AsyncMock()
    timeouts = []

    async def notifies(*, timeout=None):
        timeouts.append(timeout)
        entered.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            if len(timeouts) > 1:
                raise
            if deliver_notification:
                yield Notify("test", '{"t": "test"}', 1)

    connection.notifies = notifies
    monkeypatch.setattr(
        "chancy.worker.AsyncConnection.connect",
        AsyncMock(return_value=connection),
    )
    task = worker.manager.add("notifications", worker._maintain_notifications())
    await entered.wait()
    task.cancel()
    await assert_cancelled(task)
    assert timeouts == [1]
    connection.close.assert_awaited_once()
