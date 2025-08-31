import asyncio

import pytest

from chancy import Chancy, Worker, Queue, job


@job(queue="single")
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
            "single",
            concurrency=1,
            polling_interval=5,
            executor=Chancy.Executor.Async,
        )
    )

    all_refs = [
        ref
        async for refs in chancy.push_many([quick_job.job] * 5)
        for ref in refs
    ]

    # If the bug is present, this will take at least 25 seconds.
    await chancy.wait_for_jobs(all_refs, timeout=10)
