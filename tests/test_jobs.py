import time
import asyncio

import pytest

from chancy import Chancy, Worker, Queue, QueuedJob, Limit, job, Job
from test_worker import job_that_fails


@job()
def slow_job_to_run():
    time.sleep(5)


@job()
def job_to_run():
    pass


@job()
async def async_job_to_run():
    pass


@job()
def job_with_instance(*, context: QueuedJob):
    context.meta["received_instance"] = True


@job()
async def async_job_with_instance(*, context: QueuedJob):
    context.meta["received_instance"] = True


@job()
async def very_long_job():
    await asyncio.sleep(60 * 60)


@job()
def cooperative_time_limited_job(*, context: QueuedJob):
    assert context.time_remaining is not None
    while True:
        time.sleep(0.05)
        context.checkpoint()


@job()
async def async_time_limited_job(*, context: QueuedJob):
    assert context.time_remaining is not None
    while True:
        await asyncio.sleep(0.05)
        context.checkpoint()


@job()
def uncooperative_time_limited_job(*, context: QueuedJob):
    assert context.time_remaining is not None
    time.sleep(2)


@job()
def time_limited_job_without_context():
    raise AssertionError("Job should not have been executed.")


@job()
async def async_time_limited_job_without_context():
    raise AssertionError("Job should not have been executed.")


@job(queue="low")
async def decorated_job_to_run():
    pass


@job(queue="low")
def sync_decorated_job_to_run():
    pass


@job()
def job_with_kwarg_generic(*, hello: list[str]):
    pass


@pytest.mark.asyncio
async def test_basic_job_sync(
    chancy: Chancy, worker: Worker, sync_job_executor: str
):
    """
    Ensures that a synchronous job can run on every supporting executor.
    """
    await chancy.declare(Queue("low", executor=sync_job_executor))

    ref = await chancy.push(job_to_run.job.with_queue("low"))
    j = await chancy.wait_for_job(ref, timeout=30)

    assert j.state == j.State.SUCCEEDED


@pytest.mark.asyncio
async def test_basic_job_async(
    chancy: Chancy, worker: Worker, async_job_executor: str
):
    """
    Ensures that an asynchronous job can run on every supporting executor.
    """
    await chancy.declare(Queue("low", executor=async_job_executor))

    ref = await chancy.push(async_job_to_run.job.with_queue("low"))
    j = await chancy.wait_for_job(ref, timeout=30)

    assert j.state == j.State.SUCCEEDED


@pytest.mark.asyncio
async def test_wait_for_job_timeout(
    chancy: Chancy, worker: Worker, sync_job_executor: str
):
    """
    Ensures that waiting for a job times out as expected.
    """
    await chancy.declare(Queue("low", executor=sync_job_executor))

    ref = await chancy.push(job_to_run.job.with_queue("low"))
    j = await chancy.wait_for_job(ref, timeout=20)

    assert j.state == j.State.SUCCEEDED

    ref = await chancy.push(slow_job_to_run.job.with_queue("low"))
    with pytest.raises(asyncio.TimeoutError):
        await chancy.wait_for_job(ref, timeout=0.1)


@pytest.mark.asyncio
async def test_wait_for_jobs_ignores_purged_jobs(
    chancy: Chancy, worker: Worker
):
    """
    Ensures that waiting for jobs ignores references to purged jobs.
    """
    await chancy.declare(Queue("default"))

    existing_ref = await chancy.push(job_to_run.job)
    purged_ref = await chancy.push(job_to_run.job)

    await chancy.wait_for_job(existing_ref, timeout=30)
    await chancy.wait_for_job(purged_ref, timeout=30)
    await chancy.purge_jobs([purged_ref])

    completed = await chancy.wait_for_jobs(
        [existing_ref, purged_ref], interval=0.01, timeout=1
    )

    assert [job.id for job in completed] == [existing_ref.identifier]


@pytest.mark.asyncio
async def test_cooperative_time_limit_sync(
    chancy: Chancy,
    worker: Worker,
    cooperative_time_limit_executor: str,
):
    await chancy.declare(
        Queue(
            "time_limit_sync",
            executor=cooperative_time_limit_executor,
        )
    )
    ref = await chancy.push(
        cooperative_time_limited_job.job.with_queue(
            "time_limit_sync"
        ).with_limits([Limit(Limit.Type.TIME, 1)])
    )

    completed = await chancy.wait_for_job(ref, timeout=30)

    assert completed.state == QueuedJob.State.FAILED
    assert "TimeoutError: Job timed out." in completed.errors[-1]["traceback"]


@pytest.mark.asyncio
async def test_cooperative_time_limit_async(
    chancy: Chancy,
    worker: Worker,
    cooperative_time_limit_executor: str,
):
    await chancy.declare(
        Queue(
            "time_limit_async",
            executor=cooperative_time_limit_executor,
        )
    )
    ref = await chancy.push(
        async_time_limited_job.job.with_queue("time_limit_async").with_limits(
            [Limit(Limit.Type.TIME, 1)]
        )
    )

    completed = await chancy.wait_for_job(ref, timeout=30)

    assert completed.state == QueuedJob.State.FAILED
    assert "TimeoutError: Job timed out." in completed.errors[-1]["traceback"]


@pytest.mark.asyncio
async def test_cooperative_time_limit_checked_after_return(
    chancy: Chancy,
    worker: Worker,
    cooperative_time_limit_executor: str,
):
    await chancy.declare(
        Queue(
            "time_limit_after",
            executor=cooperative_time_limit_executor,
        )
    )
    ref = await chancy.push(
        uncooperative_time_limited_job.job.with_queue(
            "time_limit_after"
        ).with_limits([Limit(Limit.Type.TIME, 1)])
    )

    completed = await chancy.wait_for_job(ref, timeout=30)

    assert completed.state == QueuedJob.State.FAILED
    assert "TimeoutError: Job timed out." in completed.errors[-1]["traceback"]


@pytest.mark.parametrize(
    "job_without_context",
    [time_limited_job_without_context, async_time_limited_job_without_context],
)
@pytest.mark.asyncio
async def test_cooperative_time_limit_requires_context(
    chancy: Chancy,
    worker: Worker,
    cooperative_time_limit_executor: str,
    job_without_context,
):
    await chancy.declare(
        Queue(
            "time_limit_context",
            executor=cooperative_time_limit_executor,
        )
    )
    ref = await chancy.push(
        job_without_context.job.with_queue("time_limit_context").with_limits(
            [Limit(Limit.Type.TIME, 1)]
        )
    )

    completed = await chancy.wait_for_job(ref, timeout=30)

    assert completed.state == QueuedJob.State.FAILED
    assert (
        "Jobs using cooperative time limits must accept"
        in completed.errors[-1]["traceback"]
    )


@pytest.mark.asyncio
async def test_automatic_time_limit(
    chancy: Chancy,
    worker: Worker,
    automatic_time_limit_executor: str,
):
    await chancy.declare(
        Queue(
            "time_limit_automatic",
            executor=automatic_time_limit_executor,
        )
    )
    ref = await chancy.push(
        very_long_job.job.with_queue("time_limit_automatic").with_limits(
            [Limit(Limit.Type.TIME, 1)]
        )
    )

    completed = await chancy.wait_for_job(ref, timeout=30)

    assert completed.state == QueuedJob.State.FAILED
    assert "TimeoutError" in completed.errors[-1]["traceback"]


@pytest.mark.asyncio
async def test_job_instance_kwarg(
    chancy: Chancy, worker: Worker, sync_job_executor: str
):
    """
    Test that jobs requesting a QueuedJob kwarg receive the correct instance.
    """
    await chancy.declare(Queue("low", executor=sync_job_executor))

    ref = await chancy.push(job_with_instance.job.with_queue("low"))
    j = await chancy.wait_for_job(ref, timeout=30)

    assert j.state == j.State.SUCCEEDED
    assert j.meta.get("received_instance") is True


@pytest.mark.asyncio
async def test_async_job_instance_kwarg(
    chancy: Chancy, worker: Worker, async_job_executor: str
):
    """
    Test that async jobs requesting a QueuedJob kwarg receive the correct
    instance.
    """
    await chancy.declare(Queue("low", executor=async_job_executor))

    ref = await chancy.push(async_job_with_instance.job.with_queue("low"))
    j = await chancy.wait_for_job(ref, timeout=30)

    assert j.state == j.State.SUCCEEDED
    assert j.meta.get("received_instance") is True


@pytest.mark.asyncio
async def test_job_signature_with_kwarg_marker(chancy, worker):
    """
    Ensures that a job with a kwarg-only marker and a generic can have its type
    signature checked.
    """
    await chancy.declare(Queue("low"))

    ref = await chancy.push(
        job_with_kwarg_generic.job.with_queue("low").with_kwargs(hello="world")
    )
    j = await chancy.wait_for_job(ref, timeout=30)
    assert j.state == j.State.SUCCEEDED


@pytest.mark.asyncio
async def test_failing_job(chancy: Chancy, worker: Worker):
    """
    Test that a job that fails will be marked as failed.
    """
    await chancy.declare(
        Queue(
            "default",
            concurrency=1,
        ),
        upsert=True,
    )
    ref = await chancy.push(Job.from_func(job_that_fails))
    j = await chancy.wait_for_job(ref, timeout=30)
    assert j.state == QueuedJob.State.FAILED


@pytest.mark.asyncio
async def test_sync_push(chancy: Chancy, worker: Worker):
    """
    Ensure that the synchronous push method works as expected (as well as
    sync_declare).
    """
    with chancy:
        chancy.sync_declare(Queue("low"))
        ref = chancy.sync_push(job_to_run.job.with_queue("low"))

    j = await chancy.wait_for_job(ref, timeout=30)
    assert j.state == QueuedJob.State.SUCCEEDED


@pytest.mark.asyncio
async def test_job_ordering(chancy: Chancy, worker: Worker):
    """
    Ensure that jobs are run in the order they are pushed when there is a
    concurrency of 1 and no priority.
    """
    refs = []
    for i in range(30):
        refs.append(await chancy.push(job_to_run.job.with_queue("low")))
        await asyncio.sleep(0.1)

    await chancy.declare(Queue("low", concurrency=1))

    completed = []
    for ref in refs:
        j = await chancy.wait_for_job(ref, timeout=30)
        assert j.state == QueuedJob.State.SUCCEEDED
        completed.append(j)

    # Ensure each job has a completed_at ordered by the order they were
    # pushed.
    by_completed_at = sorted(completed, key=lambda x: x.completed_at)
    by_uuid7 = sorted(completed, key=lambda x: x.id)

    assert by_completed_at == by_uuid7


@pytest.mark.asyncio
async def test_retry_jobs(chancy: Chancy, worker: Worker):
    """
    Ensure that retrying a failed job requeues it and executes again.
    """
    await chancy.declare(Queue("default"))

    ref = await chancy.push(Job.from_func(job_that_fails, max_attempts=1))
    j1 = await chancy.wait_for_job(ref, timeout=30)
    assert j1 is not None
    assert j1.state == QueuedJob.State.FAILED

    errors_before = len(j1.errors)

    # Retry the job and ensure it runs again (and fails again).
    await chancy.retry_jobs([ref])

    j2 = await chancy.wait_for_job(ref, timeout=30)
    assert j2 is not None
    assert j2.state == QueuedJob.State.FAILED
    # Attempts are reset on retry and incremented by the next run.
    assert j2.attempts == 1
    # We preserve error history; another attempt should add a new error.
    assert len(j2.errors) >= errors_before + 1


@pytest.mark.asyncio
async def test_purge_jobs(
    chancy: Chancy, worker: Worker, sync_job_executor: str
):
    """
    Ensure that purging jobs removes them permanently.
    """
    # Create a succeeded job
    await chancy.declare(Queue("low", executor=sync_job_executor))
    ref_ok = await chancy.push(job_to_run.job.with_queue("low"))
    j_ok = await chancy.wait_for_job(ref_ok, timeout=30)
    assert j_ok is not None and j_ok.state == QueuedJob.State.SUCCEEDED

    # Create a failed job
    await chancy.declare(Queue("default"))
    ref_fail = await chancy.push(Job.from_func(job_that_fails, max_attempts=1))
    j_fail = await chancy.wait_for_job(ref_fail, timeout=30)
    assert j_fail is not None and j_fail.state == QueuedJob.State.FAILED

    # Purge both and ensure they're gone
    await chancy.purge_jobs([ref_ok, ref_fail])
    assert await chancy.get_job(ref_ok) is None
    assert await chancy.get_job(ref_fail) is None


@pytest.mark.asyncio
async def test_executor_job_cancellation(
    chancy: Chancy,
    worker: Worker,
    cancellation_executor: str,
):
    """
    Test that active jobs can be cancelled on every supporting executor.
    """
    await chancy.declare(Queue("cancel_test", executor=cancellation_executor))

    ref = await chancy.push(very_long_job.job.with_queue("cancel_test"))
    j = await chancy.wait_for_job(
        ref, timeout=10, states={QueuedJob.State.RUNNING}
    )
    assert j.state == j.State.RUNNING

    await chancy.cancel_job(ref)

    executor = worker._executors.get("cancel_test")
    async with asyncio.timeout(10):
        while executor.is_job_running(ref):
            await asyncio.sleep(0.1)

    j = await chancy.wait_for_job(ref, timeout=10)
    assert j.state == j.State.FAILED
