import pytest

from chancy import Worker, Chancy, Queue, QueuedJob, job


low = Queue("low", concurrency=1)
high = Queue("high", concurrency=1)


@job()
def job_to_run():
    return


@job()
def job_that_fails():
    raise ValueError("This job should fail.")


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
