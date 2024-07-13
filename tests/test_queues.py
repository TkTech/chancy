import asyncio

import pytest

from chancy import Worker, Chancy, Job, Queue, JobInstance


def job_to_run():
    return


def job_that_fails():
    raise ValueError("This job should fail.")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [
                Queue(name="low", concurrency=1),
                Queue(name="high", concurrency=1),
            ]
        }
    ],
    indirect=True,
)
async def test_multiple_queues(
    chancy: Chancy, worker: tuple[Worker, asyncio.Task]
):
    """
    Ensure that jobs can be pushed & retrieved from multiple enabled queues.
    """
    ref_low = await chancy.push("low", Job.from_func(job_to_run))
    ref_high = await chancy.push("high", Job.from_func(job_to_run))
    job_low = await ref_low.wait(chancy)
    job_high = await ref_high.wait(chancy)
    assert job_low.state == JobInstance.State.SUCCEEDED
    assert ref_low.queue.name == "low"
    assert job_high.state == JobInstance.State.SUCCEEDED
    assert ref_high.queue.name == "high"
