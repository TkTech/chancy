import asyncio

import pytest

from chancy import Worker, Chancy, Job, Queue, JobInstance


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
    await chancy.declare(
        Queue(
            "low",
            concurrency=1,
        ),
        upsert=True,
    )

    await chancy.declare(
        Queue(
            "high",
            concurrency=1,
        ),
        upsert=True,
    )

    ref_low = await chancy.push(Job.from_func(job_to_run, queue="low"))
    ref_high = await chancy.push(Job.from_func(job_to_run, queue="high"))
    job_low = await ref_low.wait()
    job_high = await ref_high.wait()
    assert job_low.state == JobInstance.State.SUCCEEDED
    assert job_high.state == JobInstance.State.SUCCEEDED
