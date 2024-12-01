import pytest

from chancy import Job, Queue, QueuedJob, Worker
from chancy.plugins.retry import RetryPlugin


def job_that_fails():
    raise ValueError("This job should fail")


def successful_job():
    pass


@pytest.mark.parametrize(
    "chancy", [{"plugins": [RetryPlugin()]}], indirect=True
)
@pytest.mark.asyncio
async def test_retry_with_default_settings(chancy, worker: Worker):
    """Test that jobs retry with default settings when no retry_settings in meta"""
    await chancy.declare(Queue("default"))

    ref = await chancy.push(Job.from_func(job_that_fails, max_attempts=3))

    job = await chancy.wait_for_job(ref, timeout=30)
    assert job.state == QueuedJob.State.FAILED
    assert job.attempts == 3
    assert len(job.errors) == 3

    # Verify error structure
    for i, error in enumerate(job.errors):
        assert error["attempt"] == i
        assert "ValueError: This job should fail" in error["traceback"]


@pytest.mark.parametrize(
    "chancy", [{"plugins": [RetryPlugin()]}], indirect=True
)
@pytest.mark.asyncio
async def test_retry_with_custom_settings(chancy, worker: Worker):
    """Test that jobs retry with custom settings from meta"""
    await chancy.declare(Queue("default"))

    ref = await chancy.push(
        Job.from_func(
            job_that_fails,
            max_attempts=3,
            meta={
                "retry_settings": {
                    "backoff": 2,
                    "backoff_factor": 3,
                    "backoff_limit": 10,
                    "backoff_jitter": [0, 1],
                }
            },
        )
    )

    job = await chancy.wait_for_job(ref, timeout=30)
    assert job.state == QueuedJob.State.FAILED
    assert job.attempts == 3
    assert len(job.errors) == 3

    # Verify the retry settings remained unchanged
    assert job.meta["retry_settings"] == {
        "backoff": 2,
        "backoff_factor": 3,
        "backoff_limit": 10,
        "backoff_jitter": [0, 1],
    }


@pytest.mark.parametrize(
    "chancy", [{"plugins": [RetryPlugin()]}], indirect=True
)
@pytest.mark.asyncio
async def test_no_retry_on_success(chancy, worker: Worker):
    """Test that successful jobs don't trigger retry logic"""
    await chancy.declare(Queue("default"))

    ref = await chancy.push(
        Job.from_func(
            successful_job,
            max_attempts=3,
            meta={"retry_settings": {"backoff": 1}},
        )
    )

    job = await chancy.wait_for_job(ref, timeout=30)
    assert job.state == QueuedJob.State.SUCCEEDED
    assert job.attempts == 1
    assert len(job.errors) == 0


@pytest.mark.parametrize(
    "chancy", [{"plugins": [RetryPlugin()]}], indirect=True
)
@pytest.mark.asyncio
async def test_respect_max_attempts(chancy, worker: Worker):
    """Test that jobs don't retry beyond max_attempts"""
    await chancy.declare(Queue("default"))

    ref = await chancy.push(
        Job.from_func(
            job_that_fails,
            max_attempts=2,
            meta={
                "retry_settings": {
                    "backoff": 1,
                    "backoff_factor": 2,
                    "backoff_limit": 10,
                    "backoff_jitter": [0, 1],
                }
            },
        )
    )

    job = await chancy.wait_for_job(ref, timeout=30)
    assert job.state == QueuedJob.State.FAILED
    assert job.attempts == 2
    assert len(job.errors) == 2
