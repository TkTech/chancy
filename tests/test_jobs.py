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
@pytest.mark.parametrize(
    "executor",
    [
        "chancy.executors.process.ProcessExecutor",
        "chancy.executors.thread.ThreadedExecutor",
    ],
)
async def test_basic_job_sync(
    chancy: Chancy, worker: tuple[Worker, asyncio.Task], executor: str
):
    """
    Ensures that a basic job can be run on all built-in executors.
    """
    await chancy.declare(Queue("low", executor=executor))

    ref = await chancy.push(Job.from_func(job_to_run, queue="low"))
    job = await chancy.wait_for_job(ref)

    assert job.state == job.State.SUCCEEDED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "executor",
    [
        "chancy.executors.asyncex.AsyncExecutor",
    ],
)
async def test_basic_job_async(
    chancy: Chancy, worker: tuple[Worker, asyncio.Task], executor: str
):
    """
    Ensures that a basic job can be run on all built-in executors.
    """
    await chancy.declare(Queue("low", executor=executor))

    ref = await chancy.push(Job.from_func(async_job_to_run, queue="low"))
    job = await chancy.wait_for_job(ref)

    assert job.state == job.State.SUCCEEDED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "executor",
    [
        "chancy.executors.process.ProcessExecutor",
        "chancy.executors.thread.ThreadedExecutor",
    ],
)
async def test_job_instance_kwarg(
    chancy: Chancy, worker: tuple[Worker, asyncio.Task], executor: str
):
    """
    Test that jobs requesting a QueuedJob kwarg receive the correct instance.
    """
    await chancy.declare(Queue("low", executor=executor))

    ref = await chancy.push(Job.from_func(job_with_instance, queue="low"))
    job = await chancy.wait_for_job(ref)

    assert job.state == job.State.SUCCEEDED
    assert job.meta.get("received_instance") is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "executor",
    [
        "chancy.executors.asyncex.AsyncExecutor",
    ],
)
async def test_async_job_instance_kwarg(
    chancy: Chancy, worker: tuple[Worker, asyncio.Task], executor: str
):
    """
    Test that async jobs requesting a QueuedJob kwarg receive the correct
    instance.
    """
    await chancy.declare(Queue("low", executor=executor))

    ref = await chancy.push(Job.from_func(async_job_with_instance, queue="low"))
    job = await chancy.wait_for_job(ref)

    assert job.state == job.State.SUCCEEDED
    assert job.meta.get("received_instance") is True
