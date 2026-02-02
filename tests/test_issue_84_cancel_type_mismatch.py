"""
Regression test for issue #84

In ProcessExecutor, pids_for_job is keyed by job.id (string), but the cancel()
method looked up by ref.identifier (UUID). Since string keys don't match UUID
keys in a dict, the lookup returned None and the SIGUSR1 signal was never sent.

https://github.com/TkTech/chancy/issues/84
"""

import time
import asyncio

import pytest

from chancy import Chancy, Worker, Queue, job, QueuedJob


@job()
def long_running_job():
    time.sleep(120)


@job()
async def async_long_running_job():
    await asyncio.sleep(120)


@pytest.mark.asyncio
async def test_process_executor_job_cancellation(
    chancy: Chancy, worker: Worker
):
    """
    Push a job, verify it's running, cancel it, verify it's cancelled.
    """
    await chancy.declare(Queue("cancel_test", executor=Chancy.Executor.Process))

    ref = await chancy.push(long_running_job.job.with_queue("cancel_test"))
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


@pytest.mark.asyncio
async def test_async_executor_job_cancellation(chancy: Chancy, worker: Worker):
    """
    Push an async job, verify it's running, cancel it, verify it's cancelled.
    """
    await chancy.declare(
        Queue("async_cancel_test", executor=Chancy.Executor.Async)
    )

    ref = await chancy.push(
        async_long_running_job.job.with_queue("async_cancel_test")
    )
    j = await chancy.wait_for_job(
        ref, timeout=10, states={QueuedJob.State.RUNNING}
    )
    assert j.state == j.State.RUNNING

    await chancy.cancel_job(ref)

    executor = worker._executors.get("async_cancel_test")
    async with asyncio.timeout(10):
        while executor.is_job_running(ref):
            await asyncio.sleep(0.1)

    j = await chancy.wait_for_job(ref, timeout=10)
    assert j.state == j.State.FAILED
