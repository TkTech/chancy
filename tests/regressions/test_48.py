import asyncio

import pytest

from chancy import Chancy, Worker, Queue, job


@job()
async def quick_job():
    await asyncio.sleep(1)


@pytest.mark.asyncio
async def test_regression_48(chancy: Chancy, worker: Worker):
    """
    When queues are configured with `concurrency=1`, it will always wait for
    `polling_interval` before fetching additional jobs, even if it's likely
    that there are more jobs to process.
    """
    await chancy.declare(
        Queue(
            "normal",
            concurrency=1,
            polling_interval=5,
            executor=Chancy.Executor.Async,
        )
    )

    all_refs = [
        ref
        async for refs in chancy.push_many(
            [quick_job.job.with_queue("normal")] * 5
        )
        for ref in refs
    ]

    # This will take 25 seconds, as the worker will process one job, then
    # wait 5 seconds before fetching the next job.
    with pytest.raises(asyncio.TimeoutError):
        await chancy.wait_for_jobs(all_refs, timeout=10)


@pytest.mark.asyncio
async def test_regression_48_eager(chancy: Chancy, worker: Worker):
    """
    When queues are configured with `concurrency=1`, it will always wait for
    `polling_interval` before fetching additional jobs, even if it's likely
    that there are more jobs to process.
    """

    await chancy.declare(
        Queue(
            "eager",
            concurrency=1,
            polling_interval=5,
            executor=Chancy.Executor.Async,
            eager_polling=True,
        )
    )

    all_refs = [
        ref
        async for refs in chancy.push_many(
            [quick_job.job.with_queue("eager")] * 5
        )
        for ref in refs
    ]

    # This should take roughly 5 seconds, as the worker will process one job,
    # then immediately fetch the next job.
    await chancy.wait_for_jobs(all_refs, timeout=10)
