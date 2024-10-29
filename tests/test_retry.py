import asyncio
from datetime import datetime, timedelta, timezone
import pytest

from chancy import Worker, Chancy, Job, Queue, JobInstance
from chancy.retry import Retry


def job_that_succeeds():
    """Simple job that always succeeds"""
    return "success"


def fail_then_succeed(*, job: JobInstance):
    """Job that fails on first attempt but succeeds on retry"""
    if job.attempts == 0:
        raise ValueError("First attempt fails")
    return "success"


def always_fails():
    """Job that always raises an error"""
    raise ValueError("Always fails")


def job_with_explicit_retry():
    """Job that explicitly requests a retry"""
    raise Retry(max_attempts=3, backoff=1, backoff_factor=2)


def eventual_success(*, job: JobInstance):
    """Job that eventually succeeds after multiple retries"""
    if job.attempts < 2:
        raise Retry(max_attempts=3)
    return "success"


def job_with_backoff():
    """Job that will retry with specific backoff"""
    raise Retry(
        max_attempts=2,
        backoff=5,  # 5 second delay
        backoff_factor=1,  # No multiplication
    )


def job_with_custom_max():
    """Job that overrides max_attempts"""
    raise Retry(max_attempts=2)


def job_with_data():
    """Job that raises Retry to test data preservation"""
    raise Retry(max_attempts=2)


@pytest.mark.asyncio
async def test_automatic_retry_on_failure(
    chancy: Chancy, worker: tuple[Worker, asyncio.Task]
):
    """Test that jobs automatically retry on failure"""
    await chancy.declare(Queue("default"))

    job = Job.from_func(fail_then_succeed, max_attempts=2)

    ref = await chancy.push(job)
    final_job = await chancy.wait_for_job(ref)

    assert final_job.state == JobInstance.State.SUCCEEDED
    assert final_job.attempts == 2
    assert len(final_job.errors) == 1
    assert "First attempt fails" in final_job.errors[0]["traceback"]


@pytest.mark.asyncio
async def test_max_attempts(
    chancy: Chancy, worker: tuple[Worker, asyncio.Task]
):
    """Test that jobs fail after max retries"""
    await chancy.declare(Queue("default"))

    job = Job.from_func(always_fails, max_attempts=3)

    ref = await chancy.push(job)
    final_job = await chancy.wait_for_job(ref)

    assert final_job.state == JobInstance.State.FAILED
    assert final_job.attempts == 3
    assert len(final_job.errors) == 3


@pytest.mark.asyncio
async def test_explicit_retry(
    chancy: Chancy, worker: tuple[Worker, asyncio.Task]
):
    """Test explicit retry with Retry exception"""
    await chancy.declare(Queue("default"))

    job = Job.from_func(job_with_explicit_retry, max_attempts=3)

    ref = await chancy.push(job)
    final_job = await chancy.wait_for_job(ref)

    assert final_job.state == JobInstance.State.FAILED
    assert final_job.attempts == 3
    assert "retry_count" in final_job.meta


@pytest.mark.asyncio
async def test_increase_max_attempts(
    chancy: Chancy, worker: tuple[Worker, asyncio.Task]
):
    """Test retry with increasing max_attempts that eventually succeeds."""
    await chancy.declare(Queue("default"))

    job = Job.from_func(eventual_success, max_attempts=1)

    ref = await chancy.push(job)
    final_job = await chancy.wait_for_job(ref)

    assert final_job.state == JobInstance.State.SUCCEEDED
    assert final_job.attempts == 3
    assert final_job.max_attempts == 3


@pytest.mark.asyncio
async def test_retry_backoff(
    chancy: Chancy, worker: tuple[Worker, asyncio.Task]
):
    """Test that retry backoff timing is respected"""
    await chancy.declare(Queue("default"))

    job = Job.from_func(job_with_backoff)
    start_time = datetime.now(timezone.utc)

    ref = await chancy.push(job)
    final_job = await chancy.wait_for_job(ref)

    duration = final_job.completed_at - start_time
    assert duration >= timedelta(seconds=5)


@pytest.mark.asyncio
async def test_retry_with_custom_max_attempts(
    chancy: Chancy, worker: tuple[Worker, asyncio.Task]
):
    """Test that Retry can override job's max_attempts"""
    await chancy.declare(Queue("default"))

    job = Job.from_func(
        job_with_custom_max,
        # This should be overwritten by the Retry exception raised in the
        # job.
        max_attempts=5,
    )

    ref = await chancy.push(job)
    final_job = await chancy.wait_for_job(ref)

    assert final_job.state == JobInstance.State.FAILED
    assert final_job.max_attempts == 2


@pytest.mark.asyncio
async def test_retry_preserves_job_data(
    chancy: Chancy, worker: tuple[Worker, asyncio.Task]
):
    """Test that job data is preserved across retries"""
    await chancy.declare(Queue("default"))

    test_data = {"key": "value"}
    job = Job.from_func(
        job_with_data,
        kwargs=test_data,
        meta={
            "original": "metadata",
        },
    )

    ref = await chancy.push(job)
    final_job = await chancy.wait_for_job(ref)

    assert final_job.kwargs == test_data
    assert final_job.meta.get("original") == "metadata"
