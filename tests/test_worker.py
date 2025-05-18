import time
import asyncio

import pytest

from chancy import Worker, Chancy, Queue, QueuedJob, job
from chancy.errors import MigrationsNeededError


@job()
def job_to_run():
    return


@job()
def job_that_fails():
    raise ValueError("This job should fail.")


@job()
def job_that_sleeps():
    time.sleep(0.5)
    return


@pytest.mark.asyncio
async def test_queue_update(chancy: Chancy, worker: Worker):
    """
    Test that updating a queue's configuration successfully reconfigures the
    executor.
    """
    await chancy.declare(
        Queue(
            "test_update",
            concurrency=1,
            executor="chancy.executors.process.ProcessExecutor",
            polling_interval=5,
        ),
        upsert=True,
    )

    ref1 = await chancy.push(job_that_sleeps.job.with_queue("test_update"))

    await asyncio.sleep(0.5)

    await chancy.declare(
        Queue(
            "test_update",
            concurrency=2,
            executor="chancy.executors.thread.ThreadedExecutor",
            polling_interval=2,
        ),
        upsert=True,
    )

    # The first job should still complete successfully
    job1 = await chancy.wait_for_job(ref1, timeout=10)
    assert job1.state == QueuedJob.State.SUCCEEDED

    # Push another job to the queue with the new configuration
    ref2 = await chancy.push(job_to_run.job.with_queue("test_update"))
    job2 = await chancy.wait_for_job(ref2, timeout=10)
    assert job2.state == QueuedJob.State.SUCCEEDED

    # Check the executor has been updated in the worker
    assert (
        worker.executors["test_update"].__class__.__name__ == "ThreadedExecutor"
    )


@pytest.mark.asyncio
async def test_queue_removal(chancy: Chancy, worker: Worker):
    """
    Test that removing a queue properly cleans up its executor.
    """
    await chancy.declare(
        Queue(
            "test_removal",
            concurrency=1,
            polling_interval=1,
        ),
        upsert=True,
    )

    # Push a job and let it complete
    ref = await chancy.push(job_to_run.job.with_queue("test_removal"))
    j = await chancy.wait_for_job(ref, timeout=30)
    assert j.state == QueuedJob.State.SUCCEEDED

    # Verify the executor exists
    assert "test_removal" in worker.executors

    await chancy.delete_queue("test_removal", purge_jobs=True)
    await worker.hub.wait_for("worker.queue.removed", timeout=30)

    # Give the executor time to clean up
    await asyncio.sleep(5)

    assert "test_removal" not in worker.executors


@pytest.mark.asyncio
async def test_error_on_needed_migrations(chancy_just_app: Chancy):
    """
    Test that an error is raised if there are migrations that need to be
    applied before starting the worker.
    """
    with pytest.raises(MigrationsNeededError):
        async with chancy_just_app:
            async with Worker(chancy_just_app):
                pass


@pytest.mark.asyncio
async def test_immediate_processing(chancy: Chancy, worker: Worker):
    """
    Test that the worker processes jobs immediately when receiving queue.pushed
    notifications instead of waiting for the full polling interval.
    """
    await chancy.declare(Queue("test_immediate", polling_interval=60))
    await worker.hub.wait_for("worker.queue.started")
    await asyncio.sleep(5)

    j = await chancy.push(job_to_run.job.with_queue("test_immediate"))

    result = await chancy.wait_for_job(
        j,
        interval=1,
        timeout=5,  # Short timeout since we expect immediate processing
    )

    assert result.state == QueuedJob.State.SUCCEEDED
