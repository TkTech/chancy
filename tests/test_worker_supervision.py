import asyncio
import gc
import signal
import traceback
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from psycopg import OperationalError
from starlette.requests import Request

from chancy import Queue, Worker
from chancy.plugin import Plugin
from chancy.plugins.api.core import CoreApiPlugin
from chancy.session import PluginSession
from tests.helpers import wait_until

pytestmark = pytest.mark.asyncio


@pytest.fixture
def unit_worker(chancy_just_app):
    chancy_just_app.plugins = {}
    unit_worker = Worker(
        chancy_just_app,
        register_signal_handlers=False,
        leadership_eligible=False,
        queue_change_poll_interval=1,
    )
    unit_worker.flush = AsyncMock()
    return unit_worker


class Observer(Plugin):
    @staticmethod
    def get_identifier():
        return "test.observer"


class BlockedCleanup:
    def __init__(self, error=None):
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.error = error

    async def __call__(self, *args, **kwargs):
        self.entered.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            await self.release.wait()
        if self.error is not None:
            raise self.error


@pytest.fixture
def expire_shutdown(unit_worker, monkeypatch):
    """Expire the next cleanup wait after the selected stage has started."""
    await_cleanup = unit_worker._await_cleanup

    def after(entered):
        async def wait(task, deadline):
            monkeypatch.setattr(unit_worker, "_await_cleanup", await_cleanup)
            await asyncio.wait_for(entered.wait(), 5)
            await await_cleanup(task, 0)

        monkeypatch.setattr(unit_worker, "_await_cleanup", wait)

    return after


async def test_failure_stops_embedded_worker_before_reporting_to_waiters(
    unit_worker,
):
    flushing, release = asyncio.Event(), asyncio.Event()

    async def flush():
        flushing.set()
        await release.wait()

    unit_worker.flush = flush
    failures = [ValueError("first"), RuntimeError("second")]
    for failure in failures:
        unit_worker._start_task("failed", AsyncMock(side_effect=failure)())
    # No shutdown waiter is needed to initiate teardown.
    await asyncio.wait_for(flushing.wait(), 1)
    assert unit_worker.shutdown_event.is_set()
    assert not unit_worker.healthy
    response = await CoreApiPlugin.health(
        Request({"type": "http"}), chancy=unit_worker.chancy, worker=unit_worker
    )
    assert response.status_code == 503
    waiters = [
        asyncio.create_task(unit_worker.wait_for_shutdown()) for _ in range(2)
    ]
    await asyncio.sleep(0)
    assert not any(waiter.done() for waiter in waiters)
    release.set()
    results = await asyncio.gather(*waiters, return_exceptions=True)
    for result in results:
        assert isinstance(result, ExceptionGroup)
        assert set(result.exceptions) == set(failures)
    with pytest.raises(ExceptionGroup) as caught:
        await unit_worker.stop()
    assert set(caught.value.exceptions) == set(failures)


@pytest.mark.parametrize("exit", ["return", "cancel", "cancel-before-start"])
async def test_unexpected_maintenance_exit_is_fatal(unit_worker, exit):
    coroutine = asyncio.sleep(0 if exit == "return" else 3600)
    task = unit_worker._start_task("maintenance", coroutine)
    if exit == "cancel":
        await asyncio.sleep(0)
        task.cancel()
    elif exit == "cancel-before-start":
        task.cancel()
    with pytest.raises(RuntimeError, match="unexpected"):
        await asyncio.wait_for(unit_worker.wait_for_shutdown(), 1)
    assert unit_worker._finalized


async def test_finite_service_and_requested_cancel_are_expected(
    unit_worker,
):
    session = PluginSession(Observer(), unit_worker)
    session.service("one-shot", AsyncMock())
    session.service("long", lambda ctx: asyncio.sleep(3600))
    session.start()
    await asyncio.sleep(0)
    await session.close()
    assert unit_worker.healthy
    assert not unit_worker.manager.errors
    assert unit_worker._stop_task is None
    await unit_worker.stop()


async def test_shutdown_timeout_keeps_resources_until_retry(
    unit_worker, expire_shutdown
):
    service = BlockedCleanup()
    cleaned = Mock()
    session = PluginSession(Observer(), unit_worker)
    session.service("stubborn", service)
    session.callback(cleaned)
    unit_worker._plugin_sessions.append(session)
    session.start()
    await service.entered.wait()
    expire_shutdown(service.entered)
    try:
        with pytest.raises(TimeoutError, match="Worker shutdown timed out"):
            await unit_worker.stop()
        cleaned.assert_not_called()
        unit_worker.flush.assert_not_awaited()
        assert session._tasks[0].cancelling() == 1
    finally:
        service.release.set()
        await unit_worker.stop()
    cleaned.assert_called_once()
    unit_worker.flush.assert_awaited_once()


@pytest.mark.parametrize(
    "stage", ["drain", "flush", "hook", "sessions", "event"]
)
async def test_shutdown_deadline_covers_every_stage(
    unit_worker, expire_shutdown, stage
):
    blocked = BlockedCleanup()

    if stage == "drain":

        async def drain():
            await blocked()
            return True

        unit_worker._drain = drain
    elif stage == "flush":
        unit_worker.flush = blocked
    elif stage == "hook":
        plugin = Observer()
        plugin.on_worker_stopped = blocked
        unit_worker._plugin_sessions.append(PluginSession(plugin, unit_worker))
    elif stage == "sessions":
        unit_worker._close_sessions = blocked
    else:
        unit_worker.hub.on("worker.stopped", blocked)

    expire_shutdown(blocked.entered)
    try:
        with pytest.raises(TimeoutError, match="Worker shutdown timed out"):
            await unit_worker.stop()
        cleanup = unit_worker._cleanup_task
        assert not cleanup.done()
        assert not unit_worker._accepting_updates
        expire_shutdown(blocked.entered)
        with pytest.raises(TimeoutError, match="Worker shutdown timed out"):
            await unit_worker.stop()
        assert unit_worker._cleanup_task is cleanup
        assert cleanup.cancelling() == 1
    finally:
        blocked.release.set()
        await unit_worker.stop()
    assert unit_worker._finalized


async def test_shutdown_stages_share_one_deadline(unit_worker):
    async def drain():
        await asyncio.sleep(0.6)
        return True

    async def flush():
        await asyncio.sleep(0.6)

    unit_worker._drain = drain
    unit_worker.flush = AsyncMock(side_effect=flush)
    try:
        with pytest.raises(TimeoutError, match="Worker shutdown timed out"):
            await asyncio.wait_for(unit_worker.stop(timeout=1), timeout=5)
        unit_worker.flush.assert_awaited_once()
    finally:
        unit_worker.flush = AsyncMock()
        await unit_worker.stop()


async def test_shutdown_waiters_share_the_original_deadline(
    unit_worker, expire_shutdown
):
    flush = BlockedCleanup()
    waiters_ready = asyncio.Event()
    unit_worker.flush = flush
    expire_shutdown(waiters_ready)
    first = asyncio.create_task(unit_worker.stop(timeout=1))
    try:
        await asyncio.wait_for(flush.entered.wait(), 5)
        second = asyncio.create_task(unit_worker.stop(timeout=10))
        waiter = asyncio.create_task(unit_worker.wait_for_shutdown())
        waiters_ready.set()
        results = await asyncio.wait_for(
            asyncio.gather(first, second, waiter, return_exceptions=True), 5
        )
        assert all(isinstance(error, TimeoutError) for error in results)
        assert results[0] is results[1] is results[2]
    finally:
        waiters_ready.set()
        flush.release.set()
        unit_worker.flush = AsyncMock()
        await asyncio.gather(first, return_exceptions=True)
        await unit_worker.stop()


async def test_stop_finishes_after_startup_cleanup_times_out(unit_worker):
    unit_worker.shutdown_timeout = 1
    release = asyncio.Event()
    drain_timeouts = []
    unit_worker.hub.on("worker.shutdown_timeout", drain_timeouts.append)
    session = PluginSession(Observer(), unit_worker)
    unit_worker._plugin_sessions.append(session)
    close = session.close

    async def blocked_close():
        try:
            await release.wait()
        except asyncio.CancelledError:
            await release.wait()
        await close()

    session.close = blocked_close
    unit_worker._start = AsyncMock(side_effect=ValueError("setup failed"))
    try:
        with pytest.raises(ExceptionGroup) as caught:
            await unit_worker.start()
        assert any(
            isinstance(error, TimeoutError) for error in caught.value.exceptions
        )
        stopping = asyncio.create_task(unit_worker.stop())
        await wait_until(lambda: unit_worker._stop_task is not None)
        release.set()
        assert await stopping is True
        assert unit_worker._finalized
        unit_worker.flush.assert_awaited_once()
        assert not drain_timeouts
    finally:
        release.set()
        await unit_worker._cleanup_task


@pytest.mark.parametrize("wait", [False, True])
async def test_context_exit_does_not_restart_timed_out_shutdown(
    unit_worker, expire_shutdown, wait
):
    unit_worker.start = AsyncMock()
    flush = BlockedCleanup()
    unit_worker.flush = AsyncMock(side_effect=flush.__call__)
    expire_shutdown(flush.entered)
    try:
        with pytest.raises(TimeoutError, match="Worker shutdown timed out"):
            async with unit_worker:
                if wait:
                    unit_worker._begin_stop(30)
                    await unit_worker.wait_for_shutdown()
                else:
                    await unit_worker.stop()
        unit_worker.flush.assert_awaited_once()
    finally:
        flush.release.set()
        unit_worker.flush = AsyncMock()
        await unit_worker.stop()


async def test_cancelled_final_flush_is_not_automatically_repeated(
    unit_worker,
):
    unit_worker.flush = AsyncMock(side_effect=asyncio.CancelledError)
    with pytest.raises(asyncio.CancelledError):
        await unit_worker.stop()
    unit_worker.flush.assert_awaited_once()


@pytest.mark.parametrize("error_type", [OperationalError, TimeoutError])
async def test_shutdown_retry_recovers_from_late_flush_error(
    unit_worker, caplog, expire_shutdown, error_type
):
    failure = error_type("cancelled flush")
    flush = BlockedCleanup(failure)
    unit_worker.flush = flush
    expire_shutdown(flush.entered)
    try:
        with pytest.raises(TimeoutError, match="Worker shutdown timed out"):
            await unit_worker.stop()
        flush.release.set()
        await asyncio.gather(unit_worker._cleanup_task, return_exceptions=True)

        unit_worker.flush = AsyncMock()
        assert await unit_worker.stop() is True
        assert await unit_worker.wait_for_shutdown() is True
        assert await unit_worker.stop() is True
        unit_worker.flush.assert_awaited_once()
        assert [
            record.exc_info[1]
            for record in caplog.records
            if record.exc_info is not None
        ] == [failure]
    finally:
        flush.release.set()
        await asyncio.gather(unit_worker._cleanup_task, return_exceptions=True)


@pytest.mark.parametrize("error_type", [OperationalError, TimeoutError])
async def test_shutdown_retry_waits_for_previous_cleanup(
    unit_worker, expire_shutdown, error_type
):
    flush = BlockedCleanup(error_type("cancelled flush"))
    unit_worker.flush = flush
    expire_shutdown(flush.entered)
    retry = None
    try:
        with pytest.raises(TimeoutError, match="Worker shutdown timed out"):
            await unit_worker.stop()

        previous = unit_worker._stop_task
        unit_worker.flush = AsyncMock()
        retry = asyncio.create_task(unit_worker.stop())
        await wait_until(lambda: unit_worker._stop_task is not previous)
        await asyncio.sleep(0)
        assert not retry.done()
        unit_worker.flush.assert_not_awaited()

        flush.release.set()
        assert await retry is True
        unit_worker.flush.assert_awaited_once()
    finally:
        flush.release.set()
        if retry is not None:
            await asyncio.gather(retry, return_exceptions=True)
        await asyncio.gather(unit_worker._cleanup_task, return_exceptions=True)


async def test_shutdown_retry_reports_its_own_flush_failure(
    unit_worker, expire_shutdown
):
    flush = BlockedCleanup(OperationalError("cancelled flush"))
    unit_worker.flush = flush
    expire_shutdown(flush.entered)
    try:
        with pytest.raises(TimeoutError, match="Worker shutdown timed out"):
            await unit_worker.stop()
        flush.release.set()
        await asyncio.gather(unit_worker._cleanup_task, return_exceptions=True)

        unit_worker.flush = AsyncMock(side_effect=ValueError("retry failed"))
        with pytest.raises(ValueError, match="retry failed"):
            await unit_worker.stop()
        unit_worker.flush.assert_awaited_once()

        unit_worker.flush = AsyncMock()
        assert await unit_worker.stop() is True
        unit_worker.flush.assert_awaited_once()
    finally:
        flush.release.set()
        await asyncio.gather(unit_worker._cleanup_task, return_exceptions=True)


async def test_late_resource_cleanup_failure_remains_reported(
    unit_worker, expire_shutdown
):
    blocked = BlockedCleanup()
    session = PluginSession(Observer(), unit_worker)
    unit_worker._plugin_sessions.append(session)
    failure = ValueError("resource cleanup failed")
    cleanup = Mock(side_effect=failure)
    session.callback(cleanup)
    close = session._close

    async def delayed_close():
        await blocked()
        await close()

    session._close = delayed_close
    expire_shutdown(blocked.entered)
    try:
        with pytest.raises(TimeoutError, match="Worker shutdown timed out"):
            await unit_worker.stop()
        blocked.release.set()
        assert session._close_task is not None
        await asyncio.gather(session._close_task, return_exceptions=True)
        for _ in range(2):
            with pytest.raises(ValueError) as caught:
                await unit_worker.stop()
            assert caught.value is failure
        assert unit_worker._finalized
        cleanup.assert_called_once()
    finally:
        blocked.release.set()
        await asyncio.gather(unit_worker._cleanup_task, return_exceptions=True)


async def test_terminal_cleanup_failure_does_not_repeat_hooks(
    unit_worker,
):
    plugin = Observer()
    plugin.on_worker_stopped = AsyncMock()
    session = PluginSession(plugin, unit_worker)
    cleanup = Mock(side_effect=ValueError("cleanup failed"))
    session.callback(cleanup)
    unit_worker._plugin_sessions.append(session)
    for _ in range(2):
        with pytest.raises(ValueError, match="cleanup failed"):
            await unit_worker.stop()
    unit_worker.flush.assert_awaited_once()
    plugin.on_worker_stopped.assert_awaited_once()
    cleanup.assert_called_once()


async def test_cancellation_failures_join_the_triggering_failure(
    unit_worker,
):
    entered = asyncio.Event()
    original = ValueError("original failure")
    cleanup = RuntimeError("task cleanup failed")

    async def service():
        entered.set()
        try:
            await asyncio.Future()
        finally:
            raise cleanup

    unit_worker._start_task("service", service())
    await entered.wait()
    unit_worker._start_task("broken", AsyncMock(side_effect=original)())
    with pytest.raises(ExceptionGroup) as caught:
        await asyncio.wait_for(unit_worker.wait_for_shutdown(), 1)
    assert set(caught.value.exceptions) == {original, cleanup}
    assert unit_worker._finalized


async def test_startup_cleanup_keeps_original_error_and_closes_every_session(
    unit_worker,
):
    first_cleanup = Mock()
    second_cleanup = Mock(side_effect=RuntimeError("cleanup failed"))
    original = ValueError("setup failed")
    first = PluginSession(Observer(), unit_worker)
    second = PluginSession(Observer(), unit_worker)
    first.callback(first_cleanup)
    second.callback(second_cleanup)
    unit_worker._plugin_sessions.extend([first, second])
    with pytest.raises(ExceptionGroup) as caught:
        await unit_worker._abort_start(original)
    first_cleanup.assert_called_once()
    second_cleanup.assert_called_once()
    assert caught.value.exceptions[0] is original
    assert str(caught.value.exceptions[1]) == "cleanup failed"
    unit_worker.flush.assert_not_awaited()


async def test_notification_handler_failure_keeps_listener_and_delivery_alive(
    unit_worker, monkeypatch
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
    unit_worker.hub.on("test", Mock(side_effect=ValueError("observer failed")))
    unit_worker.hub.on("test", lambda event: delivered.set())
    waiter = asyncio.create_task(unit_worker.hub.wait_for("test"))
    await asyncio.sleep(0)
    task = unit_worker._start_task(
        "notifications", unit_worker._maintain_notifications()
    )
    await asyncio.wait_for(delivered.wait(), 1)
    assert await waiter
    assert not task.done() and unit_worker.healthy
    await unit_worker.stop()
    connection.close.assert_awaited_once()


async def test_notification_startup_failure_cannot_hang(
    unit_worker, monkeypatch
):
    unit_worker.chancy.is_up_to_date = AsyncMock(return_value=True)
    unit_worker.chancy.declare = AsyncMock()
    for name in (
        "_maintain_queues",
        "_maintain_updates",
        "_maintain_heartbeat",
    ):
        monkeypatch.setattr(unit_worker, name, lambda: asyncio.sleep(3600))
    monkeypatch.setattr(
        unit_worker,
        "_maintain_notifications",
        AsyncMock(side_effect=ValueError("connect failed")),
    )
    with pytest.raises(ValueError, match="connect failed"):
        await asyncio.wait_for(unit_worker.start(), 1)
    assert not unit_worker.manager


async def test_queue_failure_is_fatal_without_a_waiter(unit_worker):
    unit_worker.chancy.get_all_queues = AsyncMock(
        return_value=[Queue("broken", executor="no_such_executor.Class")]
    )
    unit_worker._queues_task = unit_worker._start_task(
        "queues", unit_worker._maintain_queues()
    )
    await wait_until(lambda: unit_worker._finalized)
    assert not unit_worker.healthy and not unit_worker.manager
    with pytest.raises(ModuleNotFoundError):
        await unit_worker.wait_for_shutdown()


async def test_readded_queue_does_not_overlap_its_old_loop(unit_worker):
    queues = [Queue("test")]
    unit_worker.chancy.get_all_queues = AsyncMock(
        side_effect=lambda: list(queues)
    )
    release = asyncio.Event()
    calls = 0

    async def process(queue):
        nonlocal calls
        calls += 1
        await release.wait()
        if calls > 1:
            await asyncio.Future()

    unit_worker._maintain_queue = process
    unit_worker._queues_task = unit_worker._start_task(
        "queues", unit_worker._maintain_queues()
    )
    try:
        await wait_until(lambda: calls == 1)
        queues.clear()
        await wait_until(lambda: "test" not in unit_worker._queues)
        queues.append(Queue("test"))
        await wait_until(lambda: "test" in unit_worker._queues)
        assert calls == 1
        release.set()
        await wait_until(lambda: calls == 2)
        assert unit_worker.healthy
    finally:
        await unit_worker.stop()


@pytest.mark.parametrize("cleanup_fails", [False, True])
async def test_startup_cancellation_remains_cancellation(
    unit_worker, cleanup_fails
):
    failure = ValueError("cleanup failed")
    cleaned = Mock(side_effect=failure if cleanup_fails else None)
    session = PluginSession(Observer(), unit_worker)
    session.callback(cleaned)
    unit_worker._plugin_sessions.append(session)
    entered = asyncio.Event()

    async def start():
        entered.set()
        await asyncio.Future()

    unit_worker._start = start
    task = asyncio.create_task(unit_worker.start())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError) as caught:
        await task
    assert task.cancelled()
    cleaned.assert_called_once()
    assert caught.value.__cause__ is (failure if cleanup_fails else None)


async def test_startup_timeout_preserves_cleanup_failure(unit_worker):
    failure = ValueError("cleanup failed")
    session = PluginSession(Observer(), unit_worker)
    session.callback(Mock(side_effect=failure))
    unit_worker._plugin_sessions.append(session)
    unit_worker._start = lambda: asyncio.sleep(3600)
    with pytest.raises(TimeoutError) as caught:
        async with asyncio.timeout(0.01):
            await unit_worker.start()
    assert isinstance(caught.value.__cause__, asyncio.CancelledError)
    assert caught.value.__cause__.__cause__ is failure


@pytest.mark.parametrize("origin", ["task", "cleanup", "stop"])
async def test_repeated_worker_waiters_preserve_original_traceback(
    unit_worker, origin
):
    unit_worker._drain_result = True

    async def fail():
        raise ValueError("original failure")

    def fail_cleanup():
        raise ValueError("original failure")

    if origin == "task":
        unit_worker._start_task("failed", fail())
        await wait_until(lambda: unit_worker._stop_task is not None)
    elif origin == "cleanup":
        session = PluginSession(Observer(), unit_worker)
        session.callback(fail_cleanup)
        unit_worker._plugin_sessions.append(session)
        unit_worker._begin_stop(1)
    else:
        unit_worker.flush = fail
        unit_worker._begin_stop(1)

    traces = []
    for _ in range(3):
        with pytest.raises(ValueError) as caught:
            await unit_worker.wait_for_shutdown()
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
    unit_worker, caplog, origin
):
    unit_worker._drain_result = True
    failure = ValueError("signal failure")
    if origin == "task":
        unit_worker._start_task("failed", AsyncMock(side_effect=failure)())
        await wait_until(lambda: unit_worker.manager.errors)
    else:
        unit_worker.flush = AsyncMock(side_effect=failure)

    loop = asyncio.get_running_loop()
    unhandled = []
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda loop, ctx: unhandled.append(ctx))
    try:
        task = asyncio.create_task(unit_worker.on_signal(signal.SIGTERM))
        await wait_until(task.done)
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
            await unit_worker.wait_for_shutdown()
        assert caught.value is failure
    finally:
        loop.set_exception_handler(previous)


async def test_startup_reports_each_background_failure_once(unit_worker):
    failures = [
        ValueError("heartbeat failed"),
        RuntimeError("notifications failed"),
    ]
    unit_worker.chancy.is_up_to_date = AsyncMock(return_value=True)
    unit_worker.chancy.declare = AsyncMock()
    unit_worker._maintain_queues = lambda: asyncio.sleep(3600)
    unit_worker._maintain_updates = lambda: asyncio.sleep(3600)
    unit_worker._maintain_heartbeat = AsyncMock(side_effect=failures[0])
    unit_worker._maintain_notifications = AsyncMock(side_effect=failures[1])
    with pytest.raises(ExceptionGroup) as caught:
        await unit_worker.start()
    assert set(caught.value.exceptions) == set(failures)
    assert not list(unit_worker.manager)


async def test_stop_retry_does_not_replace_an_existing_waiters_failure(
    unit_worker,
):
    unit_worker._drain_result = True
    failure = ValueError("first attempt failed")
    entered, release = asyncio.Event(), asyncio.Event()

    async def first_attempt():
        entered.set()
        await release.wait()
        raise failure

    unit_worker.flush = first_attempt
    retry_started = asyncio.get_running_loop().create_future()

    def retry_stop(task):
        unit_worker.flush = AsyncMock()
        retry_started.set_result(asyncio.create_task(unit_worker.stop()))

    unit_worker._begin_stop(1)
    unit_worker._stop_task.add_done_callback(retry_stop)
    original = asyncio.create_task(unit_worker.wait_for_shutdown())
    await entered.wait()
    release.set()
    with pytest.raises(ValueError) as caught:
        await original
    assert caught.value is failure
    retry = await retry_started
    assert await retry is True


@pytest.mark.parametrize("cleanup_kind", ["task", "session"])
async def test_cancellation_during_startup_cleanup_remains_cancellation(
    unit_worker, cleanup_kind
):
    original = ValueError("startup failed")
    entered, release = asyncio.Event(), asyncio.Event()
    session = PluginSession(Observer(), unit_worker)
    unit_worker._plugin_sessions.append(session)

    async def cleanup():
        entered.set()
        await release.wait()

    async def service():
        try:
            await asyncio.Future()
        finally:
            await cleanup()

    if cleanup_kind == "task":
        unit_worker._start_task("service", service())
        await asyncio.sleep(0)
    else:
        session.close = cleanup
    unit_worker._start = AsyncMock(side_effect=original)
    task = asyncio.create_task(unit_worker.start())
    try:
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError) as caught:
            await task
        assert task.cancelled()
        assert caught.value.__cause__ is original
    finally:
        release.set()
        await unit_worker.manager.cancel_all(timeout=1)
        await session.close()
        await asyncio.gather(task, return_exceptions=True)
