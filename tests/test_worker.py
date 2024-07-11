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
                Queue(name="default", concurrency=1),
            ]
        }
    ],
    indirect=True,
)
async def test_basic_job(chancy: Chancy, worker: tuple[Worker, asyncio.Task]):
    """
    Simply test that we can push a job, and it runs successfully.
    """
    ref = await chancy.push("default", Job.from_func(job_to_run))
    job = await ref.wait(chancy)
    assert job.state == JobInstance.State.SUCCEEDED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [
                Queue(name="default", concurrency=1),
            ]
        }
    ],
    indirect=True,
)
async def test_failing_job(chancy: Chancy, worker: tuple[Worker, asyncio.Task]):
    """
    Test that a job that fails will be marked as failed.
    """
    ref = await chancy.push("default", Job.from_func(job_that_fails))
    job = await ref.wait(chancy)
    assert job.state == JobInstance.State.FAILED
