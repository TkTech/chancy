import asyncio

import pytest

from chancy import Worker, Chancy, Job, Queue, QueuedJob


low = Queue("low", concurrency=1)
high = Queue("high", concurrency=1)


def job_to_run():
    return


def job_that_fails():
    raise ValueError("This job should fail.")


@pytest.mark.asyncio
async def test_multiple_queues(
    chancy: Chancy, worker: tuple[Worker, asyncio.Task]
):
    """
    Ensure that jobs can be pushed & retrieved from multiple enabled queues.
    """
    await chancy.declare(low)
    await chancy.declare(high)

    ref_low = await chancy.push(Job.from_func(job_to_run, queue="low"))
    ref_high = await chancy.push(Job.from_func(job_to_run, queue="high"))

    job_low = await chancy.wait_for_job(ref_low)
    job_high = await chancy.wait_for_job(ref_high)

    assert job_low.state == QueuedJob.State.SUCCEEDED
    assert job_high.state == QueuedJob.State.SUCCEEDED


@pytest.mark.asyncio
async def test_queue_iteration(
    chancy: Chancy, worker: tuple[Worker, asyncio.Task]
):
    """
    Ensure that we can retrieve and iterate over all the queues in the
    system using the Chancy object.
    """
    await chancy.declare(low)
    await chancy.declare(high)

    async for queue in chancy:
        assert queue in (low, high)


@pytest.mark.asyncio
async def test_queue_listing(
    chancy: Chancy, worker: tuple[Worker, asyncio.Task]
):
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
async def test_get_single_queue(
    chancy: Chancy, worker: tuple[Worker, asyncio.Task]
):
    """
    Ensure that we can retrieve a single queue from the system.
    """
    await chancy.declare(low)
    await chancy.declare(high)

    queue = await chancy.get_queue("low")
    assert queue == low

    queue = await chancy.get_queue("high")
    assert queue == high
