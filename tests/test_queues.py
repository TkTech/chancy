import pytest
import datetime

from chancy import Worker, Chancy, Queue, QueuedJob, job


low = Queue("low", concurrency=1)
high = Queue("high", concurrency=1)
default_concurrency = Queue("default_concurrency")


@job()
def job_to_run():
    return


@job()
def job_that_fails():
    raise ValueError("This job should fail.")


@job()
async def async_job_to_run():
    return


@pytest.mark.asyncio
async def test_multiple_queues(chancy: Chancy, worker: Worker):
    """
    Ensure that jobs can be pushed & retrieved from multiple enabled queues.
    """
    await chancy.declare(low)
    await chancy.declare(high)

    ref_low = await chancy.push(job_to_run.job.with_queue("low"))
    ref_high = await chancy.push(job_to_run.job.with_queue("high"))

    job_low = await chancy.wait_for_job(ref_low)
    job_high = await chancy.wait_for_job(ref_high)

    assert job_low.state == QueuedJob.State.SUCCEEDED
    assert job_high.state == QueuedJob.State.SUCCEEDED


@pytest.mark.asyncio
async def test_queue_listing(chancy: Chancy):
    """
    Ensure that we can retrieve a list of all the queues in the system.
    """
    await chancy.declare(low)
    await chancy.declare(high)

    queues = await chancy.get_all_queues()
    assert len(queues) == 2
    assert low in queues
    assert high in queues


@pytest.mark.asyncio
async def test_get_single_queue(chancy: Chancy):
    """
    Ensure that we can retrieve a single queue from the system.
    """
    await chancy.declare(low)
    await chancy.declare(high)

    queue = await chancy.get_queue("low")
    assert queue == low

    queue = await chancy.get_queue("high")
    assert queue == high


@pytest.mark.asyncio
async def test_default_concurrency_sync(
    chancy: Chancy, worker: Worker, sync_executor
):
    """
    Ensure that queues with default concurrency (None) can run jobs with sync executors.

    When concurrency is None, the executor's get_default_concurrency() method
    should determine the actual concurrency level.
    """
    queue = Queue("default_concurrency_sync", executor=sync_executor)
    await chancy.declare(queue)

    job_refs = []
    for _ in range(5):
        ref = await chancy.push(
            job_to_run.job.with_queue("default_concurrency_sync")
        )
        job_refs.append(ref)

    jobs = []
    for ref in job_refs:
        j = await chancy.wait_for_job(ref)
        jobs.append(j)

    for j in jobs:
        assert j.state == QueuedJob.State.SUCCEEDED


@pytest.mark.asyncio
async def test_default_concurrency_async(
    chancy: Chancy, worker: Worker, async_executor
):
    """
    Ensure that queues with default concurrency (None) can run jobs with async executors.

    When concurrency is None, the executor's get_default_concurrency() method
    should determine the actual concurrency level.
    """
    queue = Queue("default_concurrency_async", executor=async_executor)
    await chancy.declare(queue)

    job_refs = []
    for _ in range(5):
        ref = await chancy.push(
            async_job_to_run.job.with_queue("default_concurrency_async")
        )
        job_refs.append(ref)

    jobs = []
    for ref in job_refs:
        j = await chancy.wait_for_job(ref)
        jobs.append(j)

    for j in jobs:
        assert j.state == QueuedJob.State.SUCCEEDED


@pytest.mark.asyncio
async def test_queue_resume_time_tracking(chancy: Chancy):
    """
    Ensure that paused queues with resume_at set correctly track the time
    when they should automatically resume.
    """
    resume_time = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
    paused_queue = Queue(
        "tracking_queue",
        concurrency=1,
        state=Queue.State.PAUSED,
        resume_at=resume_time,
    )

    await chancy.declare(paused_queue)
    queue = await chancy.get_queue("tracking_queue")
    assert queue.state == Queue.State.PAUSED
    assert queue.resume_at == resume_time

    await chancy.resume_queue("tracking_queue")
    queue = await chancy.get_queue("tracking_queue")
    assert queue.state == Queue.State.ACTIVE


@pytest.mark.asyncio
async def test_queue_auto_resume(chancy: Chancy, worker: Worker):
    """
    Ensure that paused queues with resume_at set in the past will automatically
    resume when processed by a worker.
    """
    paused_queue = Queue(
        "auto_resume_queue",
        concurrency=1,
        state=Queue.State.PAUSED,
        polling_interval=1,
    )

    await chancy.declare(paused_queue)

    # This job should never get processed since the queue is paused.
    ref = await chancy.push(job_to_run.job.with_queue("auto_resume_queue"))
    with pytest.raises(TimeoutError):
        await chancy.wait_for_job(ref, timeout=10)

    await chancy.pause_queue(
        "auto_resume_queue",
        resume_at=(
            datetime.datetime.now(tz=datetime.timezone.utc)
            + datetime.timedelta(seconds=10)
        ),
    )

    ref = await chancy.push(job_to_run.job.with_queue("auto_resume_queue"))
    j = await chancy.wait_for_job(ref, timeout=30)
    assert j.state == QueuedJob.State.SUCCEEDED

    # Same thing, but with a timedelta
    await chancy.pause_queue(
        "auto_resume_queue",
        resume_at=datetime.timedelta(seconds=10),
    )

    ref = await chancy.push(job_to_run.job.with_queue("auto_resume_queue"))
    j = await chancy.wait_for_job(ref, timeout=30)
    assert j.state == QueuedJob.State.SUCCEEDED
