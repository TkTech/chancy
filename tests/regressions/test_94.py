import asyncio
import logging
import os
from concurrent.futures import Future, ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
import pytest_asyncio

from chancy import Chancy, Job, Limit, Queue, QueuedJob, Worker
from chancy.executors.process import ProcessExecutor


def crash():
    os._exit(1)


def succeed():
    return "ok"


def wait_for_release(started, release):
    started.set()
    if not release.wait(timeout=30):
        raise TimeoutError("Test did not release the job")


def queued_job(func, **kwargs):
    return QueuedJob(
        func=Job.from_func(func).func,
        kwargs=kwargs,
        id=uuid4(),
        claim_id=uuid4(),
        created_at=datetime.now(tz=UTC),
        limits=[Limit(Limit.Type.TIME, 30)],
    )


@pytest_asyncio.fixture
async def process_executor():
    updates = asyncio.Queue()
    worker = SimpleNamespace(
        chancy=SimpleNamespace(plugins={}, log=logging.getLogger(__name__)),
        queue_update=updates.put,
        on_job_completed=AsyncMock(),
    )
    executor = ProcessExecutor(worker, Queue("recovery", concurrency=2))
    try:
        yield executor, updates
    finally:
        await executor.stop()


@pytest.mark.asyncio
async def test_broken_pool_completes_sibling_and_accepts_new_jobs(
    process_executor,
):
    """A real crash fails both active jobs, clears claims, and allows repair."""
    executor, updates = process_executor
    original_pool = executor.pool
    # Start both workers before testing a crash. Older CPython versions can
    # miss a newly spawned worker's exit while waiting on older sentinels.
    barrier = executor.manager.Barrier(2)
    warmup = [executor.pool.submit(barrier.wait, 10) for _ in range(2)]
    await asyncio.wait_for(
        asyncio.gather(*(asyncio.wrap_future(future) for future in warmup)),
        10,
    )
    started = executor.manager.Event()
    release = executor.manager.Event()
    sibling = queued_job(wait_for_release, started=started, release=release)
    killer = queued_job(crash)

    try:
        sibling_future = await executor.push(sibling)
        assert await asyncio.to_thread(started.wait, 10)
        # The sibling is definitely executing before the other worker dies.
        # A crashed child must not leave cancellation state behind either.
        executor.pending_cancellations[sibling.claim_id] = True
        killer_future = await executor.push(killer)

        for future in (sibling_future, killer_future):
            with pytest.raises(BrokenProcessPool):
                await asyncio.wait_for(asyncio.wrap_future(future), 10)

        completed = [
            await asyncio.wait_for(updates.get(), 10) for _ in range(2)
        ]
        assert {job.id for job in completed} == {sibling.id, killer.id}
        assert all(job.state == QueuedJob.State.FAILED for job in completed)
        assert all(job.attempts == 1 for job in completed)
        assert not executor.jobs
        assert not executor.timeouts
        assert not executor.pids_for_job.copy()
        assert not executor.pending_cancellations.copy()

        healthy = queued_job(succeed)
        future = await executor.push(healthy)
        assert executor.pool is not original_pool
        _, result = await asyncio.wait_for(asyncio.wrap_future(future), 10)
        assert result == "ok"
        completed = await asyncio.wait_for(updates.get(), 10)
        assert completed.id == healthy.id
        assert completed.state == QueuedJob.State.SUCCEEDED
        assert completed.attempts == 1
    finally:
        release.set()


@pytest.mark.asyncio
async def test_replacement_rejecting_submission_completes_once(
    process_executor, monkeypatch
):
    """An immediately broken replacement must not loop or lose completion."""
    executor, updates = process_executor
    monkeypatch.setattr(
        executor.pool, "submit", Mock(side_effect=BrokenProcessPool("old"))
    )
    replacement = Mock(spec=ProcessPoolExecutor)
    replacement.submit.side_effect = BrokenProcessPool("replacement")
    create_pool = Mock(return_value=replacement)
    monkeypatch.setattr(executor, "_create_pool", create_pool)
    starting = AsyncMock(side_effect=lambda job: job)
    monkeypatch.setattr(executor, "on_job_starting", starting)
    job = queued_job(succeed)
    other_claim = uuid4()
    executor.pids_for_job[other_claim] = 123
    executor.pending_cancellations[other_claim] = True

    future = await executor.push(job)
    assert isinstance(future.exception(), BrokenProcessPool)
    completed = await asyncio.wait_for(updates.get(), 5)
    assert completed.state == QueuedJob.State.FAILED
    assert completed.attempts == 1
    assert "replacement" in completed.errors[0]["traceback"]
    create_pool.assert_called_once_with()
    replacement.submit.assert_called_once()
    starting.assert_awaited_once_with(job)
    executor.worker.on_job_completed.assert_awaited_once()
    assert updates.empty()
    assert not executor.jobs
    assert not executor.timeouts
    assert executor.pids_for_job.copy() == {other_claim: 123}
    assert executor.pending_cancellations.copy() == {other_claim: True}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cleanup_failure", ["manager", "pids", "cancellations"]
)
@pytest.mark.parametrize("job_failed", [False, True])
async def test_cleanup_failure_preserves_completion(
    process_executor, monkeypatch, caplog, cleanup_failure, job_failed
):
    """Manager cleanup must not discard a result or leave a slot occupied."""
    executor, updates = process_executor
    job = queued_job(succeed)
    future = Future()
    if job_failed:
        future.set_exception(BrokenProcessPool("worker died"))
    else:
        future.set_result((job, "ok"))
    executor.jobs[future] = job
    timeout = asyncio.create_task(asyncio.sleep(30))
    executor.timeouts[job.claim_id] = timeout

    if cleanup_failure == "manager":
        await asyncio.to_thread(executor.manager.shutdown)
    else:
        proxy = (
            executor.pids_for_job
            if cleanup_failure == "pids"
            else executor.pending_cancellations
        )
        error = BrokenPipeError if cleanup_failure == "pids" else EOFError
        monkeypatch.setattr(
            proxy, "pop", Mock(side_effect=error("unavailable"))
        )

    executor._on_job_completed(future, asyncio.get_running_loop())
    completed = await asyncio.wait_for(updates.get(), 2)
    assert completed.id == job.id
    assert completed.state == (
        QueuedJob.State.FAILED if job_failed else QueuedJob.State.SUCCEEDED
    )
    assert completed.attempts == 1
    if job_failed:
        assert "worker died" in completed.errors[0]["traceback"]
    else:
        assert not completed.errors
    assert not executor.jobs
    assert not executor.timeouts
    await asyncio.gather(timeout, return_exceptions=True)
    assert timeout.cancelled()
    executor.worker.on_job_completed.assert_awaited_once()
    assert updates.empty()
    assert "Failed to clean up process state" in caplog.text
    assert str(job.id) in caplog.text


@pytest.mark.asyncio
async def test_worker_survives_repeated_process_crashes(
    chancy: Chancy, worker: Worker
):
    """Crashes exhaust the job's budget without killing the queue worker."""
    await chancy.declare(
        Queue(
            "process_recovery",
            concurrency=1,
            executor=Chancy.Executor.Process,
            polling_interval=0.05,
        )
    )
    ref = await chancy.push(
        Job.from_func(crash, queue="process_recovery", max_attempts=2)
    )
    failed = await chancy.wait_for_job(ref, timeout=30, interval=0.05)
    assert failed.state == QueuedJob.State.FAILED
    assert failed.attempts == failed.max_attempts == 2
    assert len(failed.errors) == 2

    ref = await chancy.push(Job.from_func(succeed, queue="process_recovery"))
    completed = await chancy.wait_for_job(ref, timeout=30, interval=0.05)
    assert completed.state == QueuedJob.State.SUCCEEDED
    assert completed.attempts == 1
    assert not worker.shutdown_event.is_set()
