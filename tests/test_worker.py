import time
import asyncio

import pytest

from chancy import Worker, Chancy, Job, Queue, QueuedJob
from chancy.errors import MigrationsNeededError


def job_to_run():
    return


def job_that_fails():
    raise ValueError("This job should fail.")


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

    ref1 = await chancy.push(
        Job.from_func(job_that_sleeps, queue="test_update")
    )

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
    ref2 = await chancy.push(Job.from_func(job_to_run, queue="test_update"))
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
    ref = await chancy.push(Job.from_func(job_to_run, queue="test_removal"))
    job = await chancy.wait_for_job(ref, timeout=30)
    assert job.state == QueuedJob.State.SUCCEEDED

    # Verify the executor exists
    assert "test_removal" in worker.executors

    await chancy.delete_queue("test_removal", purge_jobs=True)
    await worker.hub.wait_for("worker.queue.removed", timeout=30)

    # Give the executor time to clean up
    await asyncio.sleep(5)

    assert "test_removal" not in worker.executors


@pytest.mark.asyncio
async def test_error_on_needed_migrations(chancy_just_app):
    """
    Test that an error is raised if there are migrations that need to be
    applied before starting the worker.
    """
    with pytest.raises(MigrationsNeededError):
        async with chancy_just_app:
            async with Worker(chancy_just_app):
                pass
