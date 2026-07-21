import os
import time

import pytest

from chancy import Chancy, Worker, Queue, Job, QueuedJob


def killer_job():
    os._exit(1)


def normal_job():
    return "ok"


def brief_sleep_job():
    time.sleep(0.5)
    return "ok"


@pytest.mark.asyncio
async def test_regression_94_worker_survives_and_pool_recovers(
    chancy: Chancy, worker: Worker
):
    """
    A job that kills its subprocess abruptly must not permanently break the
    pool or terminate the worker. After the offending job is dead-lettered, a
    subsequent normal job on the same queue must succeed (criteria 1, 3, 4).
    """
    await chancy.declare(
        Queue(
            "proc-recover",
            concurrency=1,
            executor=Chancy.Executor.Process,
        )
    )

    killer_ref = await chancy.push(
        Job.from_func(killer_job, queue="proc-recover", max_attempts=2)
    )

    failed_job = await chancy.wait_for_job(killer_ref, timeout=120, interval=1)

    assert failed_job.state == QueuedJob.State.FAILED, (
        f"expected FAILED, got {failed_job.state}"
    )
    assert failed_job.attempts == 2, (
        f"expected 2 attempts, got {failed_job.attempts}"
    )
    assert failed_job.max_attempts == 2, (
        f"max_attempts inflated to {failed_job.max_attempts} (should stay 2)"
    )

    normal_ref = await chancy.push(
        Job.from_func(normal_job, queue="proc-recover")
    )
    normal_completed = await chancy.wait_for_job(
        normal_ref, timeout=60, interval=1
    )
    assert normal_completed.state == QueuedJob.State.SUCCEEDED, (
        f"pool did not recover — normal job ended {normal_completed.state}"
    )


@pytest.mark.asyncio
async def test_regression_94_concurrent_job_survives_broken_pool(
    chancy: Chancy, worker: Worker
):
    """
    With concurrency=2, a normal job running alongside the killer must not be
    silently lost — it should ultimately end in SUCCEEDED (criterion 2).
    """
    await chancy.declare(
        Queue(
            "proc-concurrent",
            concurrency=2,
            executor=Chancy.Executor.Process,
        )
    )

    killer_ref = await chancy.push(
        Job.from_func(killer_job, queue="proc-concurrent", max_attempts=1)
    )
    innocent_ref = await chancy.push(
        Job.from_func(brief_sleep_job, queue="proc-concurrent", max_attempts=3)
    )

    await chancy.wait_for_job(killer_ref, timeout=120, interval=1)

    innocent_job = await chancy.wait_for_job(
        innocent_ref, timeout=120, interval=1
    )
    assert innocent_job.state == QueuedJob.State.SUCCEEDED, (
        f"innocent job was not SUCCEEDED — got {innocent_job.state}"
    )
