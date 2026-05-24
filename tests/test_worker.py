import time
import asyncio

import pytest
from psycopg import OperationalError

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

    # Wait for the executor to clean up
    async with asyncio.timeout(10):
        while "test_removal" in worker.executors:
            await asyncio.sleep(0.1)

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


def test_calculate_backoff_bounds(worker_no_start: Worker):
    """
    The jittered backoff stays within ``[0, ceiling]`` and the ceiling
    grows exponentially until it hits ``backoff_max``.
    """
    worker_no_start.backoff_initial = 1.0
    worker_no_start.backoff_max = 8.0

    expected_ceilings = [1.0, 2.0, 4.0, 8.0, 8.0, 8.0]
    for failures, ceiling in enumerate(expected_ceilings):
        samples = [
            worker_no_start._calculate_backoff(failures) for _ in range(200)
        ]
        assert all(0.0 <= s <= ceiling for s in samples)
        # With 200 draws of uniform(0, ceiling) we expect a healthy spread;
        # this also guards against accidentally returning the ceiling
        # itself (no-jitter regression).
        if ceiling > 0:
            assert max(samples) > ceiling * 0.5
            assert min(samples) < ceiling * 0.5


@pytest.mark.parametrize(
    "worker",
    [
        {
            "heartbeat_poll_interval": 1,
            "backoff_initial": 0.01,
            "backoff_max": 0.05,
        }
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_heartbeat_recovers_from_transient_error(
    chancy: Chancy, worker: Worker
):
    """
    A transient ``OperationalError`` raised by ``announce_worker`` must
    not kill the heartbeat loop — it should back off, retry, and recover.
    """
    original = worker.announce_worker
    calls = 0
    failures_to_inject = 2

    async def flaky_announce(conn):
        nonlocal calls
        calls += 1
        if calls <= failures_to_inject:
            raise OperationalError("simulated transient failure")
        return await original(conn)

    worker.announce_worker = flaky_announce

    async with asyncio.timeout(15):
        while calls <= failures_to_inject:
            await asyncio.sleep(0.1)
        # One more successful call proves the loop is still alive after
        # the injected failures.
        target = calls + 1
        while calls < target:
            await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_immediate_processing(chancy: Chancy, worker: Worker):
    """
    Test that the worker processes jobs immediately when receiving queue.pushed
    notifications instead of waiting for the full polling interval.
    """
    await chancy.declare(Queue("test_immediate", polling_interval=60))
    await worker.hub.wait_for("worker.queue.started")

    j = await chancy.push(job_to_run.job.with_queue("test_immediate"))

    result = await chancy.wait_for_job(
        j,
        interval=1,
        timeout=5,  # Short timeout since we expect immediate processing
    )

    assert result.state == QueuedJob.State.SUCCEEDED
