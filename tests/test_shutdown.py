import asyncio
import dataclasses
import signal
import threading
import time
from concurrent.futures import Future
from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from psycopg import OperationalError

from chancy import Chancy, Queue, QueuedJob, Reference, Worker, job
from chancy.executors.thread import ThreadedExecutor

_job_started = threading.Event()
_job_release = threading.Event()
_zombie_started = threading.Event()
_zombie_release = threading.Event()


@job()
def _finish_during_shutdown():
    _job_started.set()
    _job_release.wait(10)


@job()
def _finish_after_shutdown():
    _zombie_started.set()
    _zombie_release.wait(10)


@job()
async def _cancel_during_shutdown():
    await asyncio.Future()


@job()
async def _wait_for_dependency(*, dsn: str, prefix: str, dependency: str):
    async with Chancy(dsn, prefix=prefix) as app:
        await app.wait_for_job(Reference(dependency), interval=0.01, timeout=10)


async def _wait_until(predicate, *, timeout: float = 5) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_stop_is_idempotent_for_concurrent_callers(
    chancy_just_app: Chancy,
):
    """Concurrent callers share one shutdown operation and notification."""
    worker = Worker(chancy_just_app, register_signal_handlers=False)
    stopped_events = []
    worker.hub.on("worker.stopped", stopped_events.append)

    results = await asyncio.gather(worker.stop(), worker.stop())

    assert results == [True, True]
    assert len(stopped_events) == 1


@pytest.mark.asyncio
async def test_wait_for_shutdown_waits_for_final_flush(
    chancy_just_app: Chancy,
    monkeypatch,
):
    """Shutdown cannot finish while its final flush is blocked."""
    worker = Worker(chancy_just_app, register_signal_handlers=False)
    flush_started = asyncio.Event()
    release_flush = asyncio.Event()

    async def blocked_flush():
        flush_started.set()
        await release_flush.wait()

    monkeypatch.setattr(worker, "flush", blocked_flush)
    stop_task = asyncio.create_task(worker.stop())

    try:
        await asyncio.wait_for(flush_started.wait(), timeout=1)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(worker.wait_for_shutdown(), timeout=0.05)
    finally:
        release_flush.set()
        await asyncio.wait_for(stop_task, timeout=1)

    await worker.wait_for_shutdown()


@pytest.mark.asyncio
async def test_stop_retries_failed_final_flush(
    chancy_just_app: Chancy,
    monkeypatch,
):
    """A failed final flush can succeed on the next stop call."""
    worker = Worker(chancy_just_app, register_signal_handlers=False)
    flush = AsyncMock(side_effect=[OperationalError("Transient failure"), None])
    monkeypatch.setattr(worker, "flush", flush)

    with pytest.raises(OperationalError, match="Transient failure"):
        await worker.stop()

    assert await worker.stop() is True
    assert flush.await_count == 2


@pytest.mark.asyncio
async def test_stop_retry_preserves_forced_shutdown_result(
    chancy_just_app: Chancy,
    monkeypatch,
):
    """Retrying persistence cannot turn a forced shutdown into a clean one."""
    worker = Worker(chancy_just_app, register_signal_handlers=False)
    drain = AsyncMock(return_value=False)
    monkeypatch.setattr(worker, "_drain", drain)
    monkeypatch.setattr(
        worker,
        "flush",
        AsyncMock(side_effect=[OperationalError("Transient failure"), None]),
    )

    with pytest.raises(OperationalError):
        await worker.stop()

    assert await worker.stop() is False
    drain.assert_awaited_once()


@pytest.mark.asyncio
async def test_only_second_signal_forces_shutdown(
    chancy_just_app: Chancy,
    monkeypatch,
):
    """Programmatic shutdown does not make the first signal a forced one."""
    worker = Worker(chancy_just_app, register_signal_handlers=False)
    forced = []
    monkeypatch.setattr(worker, "_force_shutdown", lambda: forced.append(None))

    worker.shutdown_event.set()
    await worker.on_signal(signal.SIGTERM)
    assert forced == []

    await worker.on_signal(signal.SIGTERM)
    assert forced == [None]


@pytest.mark.asyncio
async def test_shutdown_timeout_still_tears_down_worker_tasks(
    chancy_just_app: Chancy,
):
    """A timed-out graceful drain must still tear down worker tasks."""
    worker = Worker(chancy_just_app, register_signal_handlers=False)
    queue_task_cancelled = asyncio.Event()
    updates_task_cancelled = asyncio.Event()

    async def maintain_blocked_queue():
        worker._executors["blocked"] = object()
        try:
            await asyncio.Future()
        finally:
            worker._executors.pop("blocked", None)
            queue_task_cancelled.set()

    async def maintain_updates():
        try:
            await asyncio.Future()
        finally:
            updates_task_cancelled.set()

    worker.manager.add("queue_blocked", maintain_blocked_queue())
    worker.manager.add("updates", maintain_updates())
    await _wait_until(lambda: "blocked" in worker.executors)

    try:
        assert await worker.stop(timeout=0.05) is False
        assert queue_task_cancelled.is_set()
        assert updates_task_cancelled.is_set()
        assert not worker.executors
    finally:
        await worker.manager.cancel_all()
        worker._executors.clear()


@pytest.mark.asyncio
async def test_threaded_executor_shutdown_does_not_block_event_loop(
    monkeypatch,
):
    """Synchronous pool teardown must not run on the event-loop thread."""
    executor = ThreadedExecutor(object(), Queue("shutdown", concurrency=1))
    shutdown = executor.pool.shutdown
    event_loop_progressed = asyncio.Event()

    def blocking_shutdown(*_args, **_kwargs):
        time.sleep(0.05)

    monkeypatch.setattr(executor.pool, "shutdown", blocking_shutdown)
    asyncio.get_running_loop().call_soon(event_loop_progressed.set)

    try:
        await executor.stop()
        assert event_loop_progressed.is_set()
    finally:
        shutdown(wait=True, cancel_futures=True)


@pytest.mark.asyncio
async def test_forced_threaded_executor_shutdown_does_not_wait(monkeypatch):
    """Forced teardown abandons running threads without a background waiter."""
    executor = ThreadedExecutor(object(), Queue("shutdown", concurrency=1))
    shutdown = executor.pool.shutdown
    calls = []

    def record_shutdown(*, wait, cancel_futures):
        calls.append((wait, cancel_futures))

    monkeypatch.setattr(executor.pool, "shutdown", record_shutdown)

    try:
        await executor._stop_on_cancel()
        assert calls == [(False, True)]
        assert not hasattr(executor, "_shutdown_future")
    finally:
        shutdown(wait=True, cancel_futures=True)


@pytest.mark.asyncio
async def test_executor_tracks_job_until_completion_update_is_queued(
    monkeypatch,
):
    """A finished future remains in the drain until its update is queued."""
    executor = ThreadedExecutor(object(), Queue("shutdown", concurrency=1))
    shutdown = executor.pool.shutdown
    completion_started = asyncio.Event()
    release_completion = asyncio.Event()
    queued_job = QueuedJob(
        func=_finish_during_shutdown.job.func,
        id=uuid4(),
        created_at=datetime.now(tz=UTC),
    )
    future = Future()
    future.set_result((queued_job, None))
    executor.jobs[future] = queued_job

    async def on_job_completed(**_kwargs):
        completion_started.set()
        await release_completion.wait()

    monkeypatch.setattr(executor, "on_job_completed", on_job_completed)

    try:
        executor._on_job_completed(future, asyncio.get_running_loop())
        await asyncio.wait_for(completion_started.wait(), timeout=1)
        assert future in executor.jobs
    finally:
        release_completion.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        executor.jobs.pop(future, None)
        shutdown(wait=True, cancel_futures=True)


@pytest.mark.asyncio
async def test_worker_flushes_completed_job_updates_before_stopping(
    chancy: Chancy,
):
    """The worker owns and flushes updates produced during a clean drain."""
    queue_name = "shutdown_updates"
    _job_started.clear()
    _job_release.clear()

    await chancy.declare(
        Queue("default", tags={"^never$"}),
        upsert=True,
    )
    await chancy.declare(
        Queue(
            queue_name,
            executor=Chancy.Executor.Threaded,
            concurrency=1,
            polling_interval=0.01,
            tags={"^shutdown$"},
        )
    )

    worker = Worker(
        chancy,
        tags={"shutdown"},
        shutdown_timeout=2,
        register_signal_handlers=False,
    )
    worker.send_outgoing_interval = 60
    await worker.start()

    try:
        await _wait_until(lambda: queue_name in worker.executors, timeout=10)
        ref = await chancy.push(
            _finish_during_shutdown.job.with_queue(queue_name)
        )

        assert await asyncio.to_thread(_job_started.wait, 5)

        running_job = await chancy.get_job(ref)
        assert running_job.state == QueuedJob.State.RUNNING

        stop_task = asyncio.create_task(worker.stop(timeout=2))
        await _wait_until(lambda: not worker)
        assert worker.outgoing.empty()

        _job_release.set()
        assert await stop_task is True

        completed_job = await chancy.get_job(ref)
        assert completed_job.state == QueuedJob.State.SUCCEEDED
    finally:
        _job_release.set()
        await worker.stop(timeout=2)


@pytest.mark.asyncio
async def test_shutdown_persists_updates_while_jobs_drain(
    chancy: Chancy, worker_no_start: Worker
):
    """A draining job can observe another job's saved completion."""
    await chancy.declare(Queue("default", tags={"^never$"}))
    dependency = await chancy.push(_cancel_during_shutdown.job)
    completed = dataclasses.replace(
        await chancy.get_job(dependency), state=QueuedJob.State.SUCCEEDED
    )
    await chancy.declare(
        Queue("waiting", executor=Chancy.Executor.Async, polling_interval=0.01)
    )
    worker_no_start.send_outgoing_interval = 0.01
    async with worker_no_start as worker:
        waiting = await chancy.push(
            _wait_for_dependency.job.with_queue("waiting").with_kwargs(
                dsn=chancy.dsn,
                prefix=chancy.prefix,
                dependency=str(dependency.identifier),
            )
        )
        await chancy.wait_for_job(
            waiting, states={QueuedJob.State.RUNNING}, interval=0.01, timeout=5
        )

        stop_task = asyncio.create_task(worker.stop(timeout=2))
        await _wait_until(lambda: not worker)
        assert await worker.queue_update(completed)

        assert await stop_task is True
        result = await chancy.get_job(waiting)
        assert result.state == QueuedJob.State.SUCCEEDED


@pytest.mark.asyncio
async def test_worker_abandons_thread_completion_after_timeout(chancy: Chancy):
    """A thread finishing after the cutoff cannot enqueue a zombie update."""
    queue_name = "shutdown_thread_zombie"
    _zombie_started.clear()
    _zombie_release.clear()

    await chancy.declare(
        Queue("default", tags={"^never$"}),
        upsert=True,
    )
    await chancy.declare(
        Queue(
            queue_name,
            executor=Chancy.Executor.Threaded,
            concurrency=1,
            polling_interval=0.01,
            tags={"^shutdown$"},
        )
    )

    worker = Worker(
        chancy,
        tags={"shutdown"},
        register_signal_handlers=False,
    )
    await worker.start()
    executor = None

    try:
        await _wait_until(lambda: queue_name in worker.executors, timeout=10)
        executor = worker.executors[queue_name]
        ref = await chancy.push(
            _finish_after_shutdown.job.with_queue(queue_name)
        )

        assert await asyncio.to_thread(_zombie_started.wait, 5)
        assert await worker.stop(timeout=0.05) is False
        assert worker.outgoing.empty()

        _zombie_release.set()
        await _wait_until(lambda: not executor.jobs)
        assert worker.outgoing.empty()

        abandoned_job = await chancy.get_job(ref)
        assert abandoned_job.state == QueuedJob.State.RUNNING
    finally:
        _zombie_release.set()
        await worker.stop(timeout=0.05)


@pytest.mark.asyncio
async def test_worker_abandons_async_completion_after_timeout(chancy: Chancy):
    """A cancelled async job cannot enqueue an update after the cutoff."""
    queue_name = "shutdown_async_zombie"

    await chancy.declare(
        Queue("default", tags={"^never$"}),
        upsert=True,
    )
    await chancy.declare(
        Queue(
            queue_name,
            executor=Chancy.Executor.Async,
            concurrency=1,
            polling_interval=0.01,
            tags={"^shutdown$"},
        )
    )

    worker = Worker(
        chancy,
        tags={"shutdown"},
        register_signal_handlers=False,
    )
    await worker.start()

    try:
        await _wait_until(lambda: queue_name in worker.executors, timeout=10)
        ref = await chancy.push(
            _cancel_during_shutdown.job.with_queue(queue_name)
        )
        await chancy.wait_for_job(
            ref,
            states={QueuedJob.State.RUNNING},
            interval=0.01,
            timeout=10,
        )

        assert await worker.stop(timeout=0.05) is False
        assert worker.outgoing.empty()

        abandoned_job = await chancy.get_job(ref)
        assert abandoned_job.state == QueuedJob.State.RUNNING
    finally:
        await worker.stop(timeout=0.05)
