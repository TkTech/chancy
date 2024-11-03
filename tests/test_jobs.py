import asyncio

import pytest

from chancy import Chancy, Worker, Queue, Job, QueuedJob


def job_to_run():
    pass


async def async_job_to_run():
    pass


def job_with_instance(*, job: QueuedJob):
    job.meta["received_instance"] = True


async def async_job_with_instance(*, job: QueuedJob):
    job.meta["received_instance"] = True


@pytest.mark.asyncio
async def test_basic_job_sync(
    chancy: Chancy, worker: tuple[Worker, asyncio.Task], sync_executor: str
):
    """
    Ensures that a basic job can be run on all built-in executors.
    """
    await chancy.declare(Queue("low", executor=sync_executor))

    ref = await chancy.push(Job.from_func(job_to_run, queue="low"))
    job = await chancy.wait_for_job(ref)

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
    job = await chancy.wait_for_job(ref)

    assert job.state == job.State.SUCCEEDED


@pytest.mark.asyncio
async def test_job_instance_kwarg(
    chancy: Chancy, worker: tuple[Worker, asyncio.Task], sync_executor: str
):
    """
    Test that jobs requesting a QueuedJob kwarg receive the correct instance.
    """
    await chancy.declare(Queue("low", executor=sync_executor))

    ref = await chancy.push(Job.from_func(job_with_instance, queue="low"))
    job = await chancy.wait_for_job(ref)

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
    job = await chancy.wait_for_job(ref)

    assert job.state == job.State.SUCCEEDED
    assert job.meta.get("received_instance") is True
