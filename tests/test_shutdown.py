import asyncio
import threading
import time
from concurrent.futures import Future
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from chancy import Chancy, Queue, QueuedJob, Worker, job
from chancy.executors.thread import ThreadedExecutor

_job_finished = threading.Event()


@job()
def _finish_during_shutdown():
    _job_finished.set()


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
    """A completed job must not remain RUNNING after a clean shutdown."""
    queue_name = "shutdown_updates"
    _job_finished.clear()

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

        assert await asyncio.to_thread(_job_finished.wait, 5)
        await _wait_until(lambda: not worker.outgoing.empty())

        running_job = await chancy.get_job(ref)
        assert running_job.state == QueuedJob.State.RUNNING

        assert await worker.stop(timeout=2) is True

        completed_job = await chancy.get_job(ref)
        assert completed_job.state == QueuedJob.State.SUCCEEDED
    finally:
        _job_finished.set()
        await worker.stop(timeout=2)
