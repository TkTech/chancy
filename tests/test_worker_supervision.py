import asyncio
import gc
import signal
import traceback
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from chancy import Queue, Worker
from chancy.plugin import Plugin
from chancy.plugins.api.core import CoreApiPlugin
from chancy.session import PluginSession

pytestmark = pytest.mark.asyncio


@pytest.fixture
def worker(chancy_just_app):
    chancy_just_app.plugins = {}
    worker = Worker(
        chancy_just_app,
        register_signal_handlers=False,
        leadership_eligible=False,
        teardown_timeout=0.02,
        queue_change_poll_interval=0.005,
    )
    worker.flush = AsyncMock()
    return worker


class Observer(Plugin):
    @staticmethod
    def get_identifier():
        return "test.observer"


async def eventually(predicate):
    async with asyncio.timeout(2):
        while not predicate():
            await asyncio.sleep(0.001)


async def test_failure_stops_embedded_worker_before_reporting_to_waiters(
    worker,
):
    flushing, release = asyncio.Event(), asyncio.Event()

    async def flush():
        flushing.set()
        await release.wait()

    worker.flush = flush
    failures = [ValueError("first"), RuntimeError("second")]
    for failure in failures:
        worker._start_task("failed", AsyncMock(side_effect=failure)())
    # No shutdown waiter is needed to initiate teardown.
    await asyncio.wait_for(flushing.wait(), 1)
    assert worker.shutdown_event.is_set()
    assert not worker.healthy
    response = await CoreApiPlugin.health(
        None, chancy=worker.chancy, worker=worker
    )
    assert response.status_code == 503
    waiters = [
        asyncio.create_task(worker.wait_for_shutdown()) for _ in range(2)
    ]
    await asyncio.sleep(0)
    assert not any(waiter.done() for waiter in waiters)
    release.set()
    results = await asyncio.gather(*waiters, return_exceptions=True)
    for result in results:
        assert isinstance(result, ExceptionGroup)
        assert set(result.exceptions) == set(failures)
    with pytest.raises(ExceptionGroup) as caught:
        await worker.stop()
    assert set(caught.value.exceptions) == set(failures)


@pytest.mark.parametrize("exit", ["return", "cancel", "cancel-before-start"])
async def test_unexpected_maintenance_exit_is_fatal(worker, exit):
    coroutine = asyncio.sleep(0 if exit == "return" else 3600)
    task = worker._start_task("maintenance", coroutine)
    if exit == "cancel":
        await asyncio.sleep(0)
        task.cancel()
    elif exit == "cancel-before-start":
        task.cancel()
    with pytest.raises(RuntimeError, match="unexpected"):
        await asyncio.wait_for(worker.wait_for_shutdown(), 1)
    assert worker._finalized


async def test_finite_service_and_requested_cancel_are_expected(worker):
    session = PluginSession(Observer(), worker)
    session.service("one-shot", AsyncMock())
    session.service("long", lambda ctx: asyncio.sleep(3600))
    session.start()
    await asyncio.sleep(0)
    await session.close()
    assert worker.healthy
    assert not worker.manager.errors
    assert worker._stop_task is None
    await worker.stop()


async def test_teardown_timeout_keeps_resources_until_retry(worker):
    release, entered = asyncio.Event(), asyncio.Event()
    cleaned = Mock()
    session = PluginSession(Observer(), worker)

    async def service(ctx):
        entered.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            await release.wait()

    session.service("stubborn", service)
    session.callback(cleaned)
    worker._plugin_sessions.append(session)
    session.start()
    await entered.wait()
    try:
        with pytest.raises(TimeoutError, match="stubborn"):
            await worker.stop(timeout=0.01)
        cleaned.assert_not_called()
        worker.flush.assert_not_awaited()
        assert session._tasks[0].cancelling() == 1
    finally:
        release.set()
        await worker.stop()
    cleaned.assert_called_once()
    worker.flush.assert_awaited_once()


async def test_terminal_cleanup_failure_does_not_repeat_hooks(worker):
    plugin = Observer()
    plugin.on_worker_stopped = AsyncMock()
    session = PluginSession(plugin, worker)
    cleanup = Mock(side_effect=ValueError("cleanup failed"))
    session.callback(cleanup)
    worker._plugin_sessions.append(session)
    for _ in range(2):
        with pytest.raises(ValueError, match="cleanup failed"):
            await worker.stop()
    worker.flush.assert_awaited_once()
    plugin.on_worker_stopped.assert_awaited_once()
    cleanup.assert_called_once()


async def test_cancellation_failures_join_the_triggering_failure(worker):
    entered = asyncio.Event()
    original = ValueError("original failure")
    cleanup = RuntimeError("task cleanup failed")

    async def service():
        entered.set()
        try:
            await asyncio.Future()
        finally:
            raise cleanup

    worker._start_task("service", service())
    await entered.wait()
    worker._start_task("broken", AsyncMock(side_effect=original)())
    with pytest.raises(ExceptionGroup) as caught:
        await asyncio.wait_for(worker.wait_for_shutdown(), 1)
    assert set(caught.value.exceptions) == {original, cleanup}
    assert worker._finalized


async def test_startup_cleanup_keeps_original_error_and_closes_every_session(
    worker,
):
    first_cleanup = Mock()
    second_cleanup = Mock(side_effect=RuntimeError("cleanup failed"))
    original = ValueError("setup failed")
    first = PluginSession(Observer(), worker)
    second = PluginSession(Observer(), worker)
    first.callback(first_cleanup)
    second.callback(second_cleanup)
    worker._plugin_sessions.extend([first, second])
    with pytest.raises(ExceptionGroup) as caught:
        await worker._abort_start(original)
    first_cleanup.assert_called_once()
    second_cleanup.assert_called_once()
    assert caught.value.exceptions[0] is original
    assert str(caught.value.exceptions[1]) == "cleanup failed"
    worker.flush.assert_not_awaited()


async def test_notification_handler_failure_keeps_listener_and_delivery_alive(
    worker, monkeypatch
):
    delivered = asyncio.Event()
    release = asyncio.Event()

    async def notifications():
        yield SimpleNamespace(payload='{"t":"test"}')
        await release.wait()

    connection = SimpleNamespace(
        execute=AsyncMock(), notifies=notifications, close=AsyncMock()
    )
    monkeypatch.setattr(
        "chancy.worker.AsyncConnection.connect",
        AsyncMock(return_value=connection),
    )
    worker.hub.on("test", Mock(side_effect=ValueError("observer failed")))
    worker.hub.on("test", lambda event: delivered.set())
    waiter = asyncio.create_task(worker.hub.wait_for("test"))
    await asyncio.sleep(0)
    task = worker._start_task("notifications", worker._maintain_notifications())
    await asyncio.wait_for(delivered.wait(), 1)
    assert await waiter
    assert not task.done() and worker.healthy
    await worker.stop()
    connection.close.assert_awaited_once()


async def test_notification_startup_failure_cannot_hang(worker, monkeypatch):
    worker.chancy.is_up_to_date = AsyncMock(return_value=True)
    worker.chancy.declare = AsyncMock()
    for name in (
        "_maintain_queues",
        "_maintain_updates",
        "_maintain_heartbeat",
    ):
        monkeypatch.setattr(worker, name, lambda: asyncio.sleep(3600))
    monkeypatch.setattr(
        worker,
        "_maintain_notifications",
        AsyncMock(side_effect=ValueError("connect failed")),
    )
    with pytest.raises(ValueError, match="connect failed"):
        await asyncio.wait_for(worker.start(), 1)
    assert not worker.manager


async def test_queue_failure_is_fatal_without_a_waiter(worker):
    worker.chancy.get_all_queues = AsyncMock(
        return_value=[Queue("broken", executor="no_such_executor.Class")]
    )
    worker._queues_task = worker._start_task(
        "queues", worker._maintain_queues()
    )
    await eventually(lambda: worker._finalized)
    assert not worker.healthy and not worker.manager
    with pytest.raises(ModuleNotFoundError):
        await worker.wait_for_shutdown()


async def test_readded_queue_does_not_overlap_its_old_loop(worker):
    queues = [Queue("test")]
    worker.chancy.get_all_queues = AsyncMock(side_effect=lambda: list(queues))
    release = asyncio.Event()
    calls = 0

    async def process(queue):
        nonlocal calls
        calls += 1
        await release.wait()
        if calls > 1:
            await asyncio.Future()

    worker._maintain_queue = process
    worker._queues_task = worker._start_task(
        "queues", worker._maintain_queues()
    )
    try:
        await eventually(lambda: calls == 1)
        queues.clear()
        await eventually(lambda: "test" not in worker._queues)
        queues.append(Queue("test"))
        await eventually(lambda: "test" in worker._queues)
        assert calls == 1
        release.set()
        await eventually(lambda: calls == 2)
        assert worker.healthy
    finally:
        await worker.stop()


@pytest.mark.parametrize("cleanup_fails", [False, True])
async def test_startup_cancellation_remains_cancellation(worker, cleanup_fails):
    failure = ValueError("cleanup failed")
    cleaned = Mock(side_effect=failure if cleanup_fails else None)
    session = PluginSession(Observer(), worker)
    session.callback(cleaned)
    worker._plugin_sessions.append(session)
    entered = asyncio.Event()

    async def start():
        entered.set()
        await asyncio.Future()

    worker._start = start
    task = asyncio.create_task(worker.start())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError) as caught:
        await task
    assert task.cancelled()
    cleaned.assert_called_once()
    assert caught.value.__cause__ is (failure if cleanup_fails else None)


async def test_startup_timeout_preserves_cleanup_failure(worker):
    failure = ValueError("cleanup failed")
    session = PluginSession(Observer(), worker)
    session.callback(Mock(side_effect=failure))
    worker._plugin_sessions.append(session)
    worker._start = lambda: asyncio.sleep(3600)
    with pytest.raises(TimeoutError) as caught:
        async with asyncio.timeout(0.01):
            await worker.start()
    assert isinstance(caught.value.__cause__, asyncio.CancelledError)
    assert caught.value.__cause__.__cause__ is failure


@pytest.mark.parametrize("origin", ["task", "cleanup", "stop"])
async def test_repeated_worker_waiters_preserve_original_traceback(
    worker, origin
):
    worker._drain_result = True

    async def fail():
        raise ValueError("original failure")

    def fail_cleanup():
        raise ValueError("original failure")

    if origin == "task":
        worker._start_task("failed", fail())
        await eventually(lambda: worker._stop_task is not None)
    elif origin == "cleanup":
        session = PluginSession(Observer(), worker)
        session.callback(fail_cleanup)
        worker._plugin_sessions.append(session)
        worker._begin_stop(1)
    else:
        worker.flush = fail
        worker._begin_stop(1)

    traces = []
    for _ in range(3):
        with pytest.raises(ValueError) as caught:
            await worker.wait_for_shutdown()
        traces.append(
            [
                frame.name
                for frame in traceback.extract_tb(caught.value.__traceback__)
            ]
        )
    assert traces[0] == traces[1] == traces[2]
    assert (
        traces[0].count("fail_cleanup" if origin == "cleanup" else "fail") == 1
    )


@pytest.mark.parametrize("origin", ["task", "stop"])
async def test_signal_consumes_already_reported_failures(
    worker, caplog, origin
):
    worker._drain_result = True
    failure = ValueError("signal failure")
    if origin == "task":
        worker._start_task("failed", AsyncMock(side_effect=failure)())
        await eventually(lambda: worker.manager.errors)
    else:
        worker.flush = AsyncMock(side_effect=failure)

    loop = asyncio.get_running_loop()
    unhandled = []
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda loop, ctx: unhandled.append(ctx))
    try:
        task = asyncio.create_task(worker.on_signal(signal.SIGTERM))
        await eventually(task.done)
        del task
        gc.collect()
        await asyncio.sleep(0)
        assert not unhandled
        assert (
            sum(
                record.exc_info is not None and record.exc_info[1] is failure
                for record in caplog.records
            )
            == 1
        )
        with pytest.raises(ValueError) as caught:
            await worker.wait_for_shutdown()
        assert caught.value is failure
    finally:
        loop.set_exception_handler(previous)


async def test_startup_reports_each_background_failure_once(worker):
    failures = [
        ValueError("heartbeat failed"),
        RuntimeError("notifications failed"),
    ]
    worker.chancy.is_up_to_date = AsyncMock(return_value=True)
    worker.chancy.declare = AsyncMock()
    worker._maintain_queues = lambda: asyncio.sleep(3600)
    worker._maintain_updates = lambda: asyncio.sleep(3600)
    worker._maintain_heartbeat = AsyncMock(side_effect=failures[0])
    worker._maintain_notifications = AsyncMock(side_effect=failures[1])
    with pytest.raises(ExceptionGroup) as caught:
        await worker.start()
    assert set(caught.value.exceptions) == set(failures)
    assert not list(worker.manager)


async def test_stop_retry_does_not_replace_an_existing_waiters_failure(worker):
    worker._drain_result = True
    failure = ValueError("first attempt failed")
    entered, release = asyncio.Event(), asyncio.Event()

    async def first_attempt():
        entered.set()
        await release.wait()
        raise failure

    worker.flush = first_attempt
    retry = None

    def retry_stop(task):
        nonlocal retry
        worker.flush = AsyncMock()
        retry = asyncio.create_task(worker.stop())

    worker._begin_stop(1)
    worker._stop_task.add_done_callback(retry_stop)
    original = asyncio.create_task(worker.wait_for_shutdown())
    await entered.wait()
    release.set()
    with pytest.raises(ValueError) as caught:
        await original
    assert caught.value is failure
    assert retry is not None
    assert await retry is True


@pytest.mark.parametrize("cleanup_kind", ["task", "session"])
async def test_cancellation_during_startup_cleanup_remains_cancellation(
    worker, cleanup_kind
):
    original = ValueError("startup failed")
    entered, release = asyncio.Event(), asyncio.Event()
    session = PluginSession(Observer(), worker)
    worker._plugin_sessions.append(session)

    async def cleanup():
        entered.set()
        await release.wait()

    async def service():
        try:
            await asyncio.Future()
        finally:
            await cleanup()

    if cleanup_kind == "task":
        worker._start_task("service", service())
        await asyncio.sleep(0)
    else:
        session.close = cleanup
    worker._start = AsyncMock(side_effect=original)
    task = asyncio.create_task(worker.start())
    try:
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError) as caught:
            await task
        assert task.cancelled()
        assert caught.value.__cause__ is original
    finally:
        release.set()
        await worker.manager.cancel_all(timeout=1)
        await session.close()
        await asyncio.gather(task, return_exceptions=True)
