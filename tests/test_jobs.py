import time
import asyncio

import pytest

from chancy import Chancy, Worker, Queue, Job, QueuedJob, Reference
from chancy.job import job


def slow_job_to_run():
    time.sleep(5)


def job_to_run():
    pass


async def async_job_to_run():
    pass


def job_with_instance(*, job: QueuedJob):
    job.meta["received_instance"] = True


async def async_job_with_instance(*, job: QueuedJob):
    job.meta["received_instance"] = True


async def very_long_job():
    await asyncio.sleep(60 * 60)


def sync_very_long_job():
    time.sleep(60)


@job(queue="low")
async def decorated_job_to_run():
    pass


@job(queue="low")
def sync_decorated_job_to_run():
    pass


@pytest.mark.asyncio
async def test_basic_job_sync(
    chancy: Chancy, worker: tuple[Worker, asyncio.Task], sync_executor: str
):
    """
    Ensures that a basic job can be run on all built-in executors.
    """
    await chancy.declare(Queue("low", executor=sync_executor))

    ref = await chancy.push(Job.from_func(job_to_run, queue="low"))
    job = await chancy.wait_for_job(ref, timeout=30)

    assert job.state == job.State.SUCCEEDED


@pytest.mark.asyncio
async def test_basic_job_async(
    chancy: Chancy, worker: tuple[Worker, asyncio.Task], async_executor: str
):
    """
    Ensures that a basic job can be run on all built-in executors.
    """
    await chancy.declare(Queue("low", executor=async_executor))

    ref = await chancy.push(Job.from_func(async_job_to_run, queue="low"))
    job = await chancy.wait_for_job(ref, timeout=30)

    assert job.state == job.State.SUCCEEDED


@pytest.mark.asyncio
async def test_wait_for_job_timeout(
    chancy: Chancy, worker: tuple[Worker, asyncio.Task], sync_executor: str
):
    """
    Ensures that waiting for a job times out as expected.
    """
    await chancy.declare(Queue("low", executor=sync_executor))

    ref = await chancy.push(Job.from_func(job_to_run, queue="low"))
    job = await chancy.wait_for_job(ref, timeout=10)

    assert job.state == job.State.SUCCEEDED

    ref = await chancy.push(Job.from_func(slow_job_to_run, queue="low"))
    with pytest.raises(asyncio.TimeoutError):
        await chancy.wait_for_job(ref, timeout=0.1)


@pytest.mark.asyncio
async def test_job_instance_kwarg(
    chancy: Chancy, worker: tuple[Worker, asyncio.Task], sync_executor: str
):
    """
    Test that jobs requesting a QueuedJob kwarg receive the correct instance.
    """
    await chancy.declare(Queue("low", executor=sync_executor))

    ref = await chancy.push(Job.from_func(job_with_instance, queue="low"))
    job = await chancy.wait_for_job(ref, timeout=30)

    assert job.state == job.State.SUCCEEDED
    assert job.meta.get("received_instance") is True


@pytest.mark.asyncio
async def test_async_job_instance_kwarg(
    chancy: Chancy, worker: tuple[Worker, asyncio.Task], async_executor: str
):
    """
    Test that async jobs requesting a QueuedJob kwarg receive the correct
    instance.
    """
    await chancy.declare(Queue("low", executor=async_executor))

    ref = await chancy.push(Job.from_func(async_job_with_instance, queue="low"))
    job = await chancy.wait_for_job(ref, timeout=30)

    assert job.state == job.State.SUCCEEDED
    assert job.meta.get("received_instance") is True


@pytest.mark.asyncio
async def test_job_cancellation(
    chancy: Chancy, worker: tuple[Worker, asyncio.Task]
):
    """
    Test that jobs can be cancelled on supporting executors.
    """

    async def cancel_in_a_bit(to_cancel: Reference):
        await asyncio.sleep(10)
        await chancy.cancel_job(to_cancel)

    await chancy.declare(Queue("async", executor=Chancy.Executor.Async))
    await chancy.declare(Queue("sync", executor=Chancy.Executor.Process))

    ref = await chancy.push(Job.from_func(very_long_job, queue="async"))
    asyncio.create_task(cancel_in_a_bit(ref))
    job = await chancy.wait_for_job(ref, timeout=30)
    assert job.state == job.State.FAILED

    ref = await chancy.push(Job.from_func(sync_very_long_job, queue="sync"))
    asyncio.create_task(cancel_in_a_bit(ref))
    job = await chancy.wait_for_job(ref, timeout=30)
    assert job.state == job.State.FAILED


@pytest.mark.asyncio
async def test_job_decorator(
    chancy: Chancy, worker: tuple[Worker, asyncio.Task], sync_executor: str
):
    """
    Test that jobs can be decorated with the @job decorator.
    """
    await chancy.declare(Queue("low", executor=sync_executor))
    ref = await chancy.push(sync_decorated_job_to_run)
    job = await chancy.wait_for_job(ref, timeout=30)
    assert job.state == job.State.SUCCEEDED


@pytest.mark.asyncio
async def test_job_decorator_async(
    chancy: Chancy, worker: tuple[Worker, asyncio.Task], async_executor: str
):
    """
    Test that async jobs can be decorated with the @job decorator.
    """
    await chancy.declare(Queue("low", executor=async_executor))
    ref = await chancy.push(decorated_job_to_run)
    job = await chancy.wait_for_job(ref, timeout=30)
    assert job.state == job.State.SUCCEEDED
