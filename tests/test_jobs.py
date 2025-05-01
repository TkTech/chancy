import time
import asyncio

import pytest

from chancy import Chancy, Worker, Queue, QueuedJob, Reference, job, Job
from test_worker import job_that_fails


@job()
def slow_job_to_run():
    time.sleep(5)


@job()
def job_to_run():
    pass


@job()
async def async_job_to_run():
    pass


@job()
def job_with_instance(*, context: QueuedJob):
    context.meta["received_instance"] = True


@job()
async def async_job_with_instance(*, context: QueuedJob):
    context.meta["received_instance"] = True


@job()
async def very_long_job():
    await asyncio.sleep(60 * 60)


@job()
def sync_very_long_job():
    time.sleep(60)


@job(queue="low")
async def decorated_job_to_run():
    pass


@job(queue="low")
def sync_decorated_job_to_run():
    pass


@job()
def job_with_kwarg_generic(*, hello: list[str]):
    pass


@pytest.mark.asyncio
async def test_basic_job_sync(
    chancy: Chancy, worker: Worker, sync_executor: str
):
    """
    Ensures that a basic job can be run on all built-in executors.
    """
    await chancy.declare(Queue("low", executor=sync_executor))

    ref = await chancy.push(job_to_run.job.with_queue("low"))
    j = await chancy.wait_for_job(ref, timeout=30)

    assert j.state == j.State.SUCCEEDED


@pytest.mark.asyncio
async def test_basic_job_async(
    chancy: Chancy, worker: Worker, async_executor: str
):
    """
    Ensures that a basic job can be run on all built-in executors.
    """
    await chancy.declare(Queue("low", executor=async_executor))

    ref = await chancy.push(async_job_to_run.job.with_queue("low"))
    j = await chancy.wait_for_job(ref, timeout=30)

    assert j.state == j.State.SUCCEEDED


@pytest.mark.asyncio
async def test_async_job_on_sync_executor(
    chancy: Chancy, worker: Worker, sync_executor: str
):
    """
    Ensures that async jobs can run on sync executors.
    """
    await chancy.declare(Queue("low", executor=sync_executor))

    ref = await chancy.push(async_job_to_run.job.with_queue("low"))
    j = await chancy.wait_for_job(ref, timeout=30)

    assert j.state == j.State.SUCCEEDED


@pytest.mark.asyncio
async def test_wait_for_job_timeout(
    chancy: Chancy, worker: Worker, sync_executor: str
):
    """
    Ensures that waiting for a job times out as expected.
    """
    await chancy.declare(Queue("low", executor=sync_executor))

    ref = await chancy.push(job_to_run.job.with_queue("low"))
    j = await chancy.wait_for_job(ref, timeout=20)

    assert j.state == j.State.SUCCEEDED

    ref = await chancy.push(slow_job_to_run.job.with_queue("low"))
    with pytest.raises(asyncio.TimeoutError):
        await chancy.wait_for_job(ref, timeout=0.1)


@pytest.mark.asyncio
async def test_job_instance_kwarg(
    chancy: Chancy, worker: Worker, sync_executor: str
):
    """
    Test that jobs requesting a QueuedJob kwarg receive the correct instance.
    """
    await chancy.declare(Queue("low", executor=sync_executor))

    ref = await chancy.push(job_with_instance.job.with_queue("low"))
    j = await chancy.wait_for_job(ref, timeout=30)

    assert j.state == j.State.SUCCEEDED
    assert j.meta.get("received_instance") is True


@pytest.mark.asyncio
async def test_async_job_instance_kwarg(
    chancy: Chancy, worker: Worker, async_executor: str
):
    """
    Test that async jobs requesting a QueuedJob kwarg receive the correct
    instance.
    """
    await chancy.declare(Queue("low", executor=async_executor))

    ref = await chancy.push(async_job_with_instance.job.with_queue("low"))
    j = await chancy.wait_for_job(ref, timeout=30)

    assert j.state == j.State.SUCCEEDED
    assert j.meta.get("received_instance") is True


@pytest.mark.asyncio
async def test_async_job_instance_kwarg_on_sync_executor(
    chancy: Chancy, worker: Worker, sync_executor: str
):
    """
    Test that async jobs requesting a QueuedJob kwarg receive the correct
    instance when run on a sync executor.
    """
    await chancy.declare(Queue("low", executor=sync_executor))

    ref = await chancy.push(async_job_with_instance.job.with_queue("low"))
    j = await chancy.wait_for_job(ref, timeout=30)

    assert j.state == j.State.SUCCEEDED
    assert j.meta.get("received_instance") is True


@pytest.mark.asyncio
async def test_job_cancellation(chancy: Chancy, worker: Worker):
    """
    Test that jobs can be cancelled on supporting executors.
    """

    async def cancel_in_a_bit(to_cancel: Reference):
        await asyncio.sleep(10)
        await chancy.cancel_job(to_cancel)

    await chancy.declare(Queue("async", executor=Chancy.Executor.Async))
    await chancy.declare(Queue("sync", executor=Chancy.Executor.Process))

    ref = await chancy.push(very_long_job.job.with_queue("async"))
    asyncio.create_task(cancel_in_a_bit(ref))
    j = await chancy.wait_for_job(ref, timeout=30)
    assert j.state == j.State.FAILED

    ref = await chancy.push(sync_very_long_job.job.with_queue("sync"))
    asyncio.create_task(cancel_in_a_bit(ref))
    j = await chancy.wait_for_job(ref, timeout=30)
    assert j.state == j.State.FAILED


@pytest.mark.asyncio
async def test_job_signature_with_kwarg_marker(chancy, worker):
    """
    Ensures that a job with a kwarg-only marker and a generic can have its type
    signature checked.
    """
    await chancy.declare(Queue("low"))

    ref = await chancy.push(
        job_with_kwarg_generic.job.with_queue("low").with_kwargs(hello="world")
    )
    j = await chancy.wait_for_job(ref, timeout=30)
    assert j.state == j.State.SUCCEEDED


@pytest.mark.asyncio
async def test_failing_job(chancy: Chancy, worker: Worker):
    """
    Test that a job that fails will be marked as failed.
    """
    await chancy.declare(
        Queue(
            "default",
            concurrency=1,
        ),
        upsert=True,
    )
    ref = await chancy.push(Job.from_func(job_that_fails))
    j = await chancy.wait_for_job(ref, timeout=30)
    assert j.state == QueuedJob.State.FAILED


@pytest.mark.asyncio
async def test_sync_push(chancy: Chancy, worker: Worker):
    """
    Ensure that the synchronous push method works as expected (as well as
    sync_declare).
    """
    with chancy:
        chancy.sync_declare(Queue("low"))
        ref = chancy.sync_push(job_to_run.job.with_queue("low"))

    j = await chancy.wait_for_job(ref, timeout=30)
    assert j.state == QueuedJob.State.SUCCEEDED


@pytest.mark.asyncio
async def test_job_ordering(chancy: Chancy, worker: Worker):
    """
    Ensure that jobs are run in the order they are pushed when there is a
    concurrency of 1 and no priority.
    """
    refs = []
    for i in range(30):
        refs.append(await chancy.push(job_to_run.job.with_queue("low")))
        await asyncio.sleep(0.1)

    await chancy.declare(Queue("low", concurrency=1))

    completed = []
    for ref in refs:
        j = await chancy.wait_for_job(ref, timeout=30)
        assert j.state == QueuedJob.State.SUCCEEDED
        completed.append(j)

    # Ensure each job has a completed_at ordered by the order they were
    # pushed.
    by_completed_at = sorted(completed, key=lambda x: x.completed_at)
    by_uuid7 = sorted(completed, key=lambda x: x.id)

    assert by_completed_at == by_uuid7
