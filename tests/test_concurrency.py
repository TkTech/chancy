import asyncio
import time

import pytest

from chancy import Chancy, Job, QueuedJob, Worker, job
from chancy.job import ConcurrencyRule


@job()
def simple_job():
    """A simple job for testing"""
    pass


@job()
def slow_job(user_id: str, action: str = "default", duration: float = 0.5):
    """A job that takes some time to complete."""
    time.sleep(duration)


async def _count_running_jobs_for_key(
    chancy: Chancy, concurrency_key: str
) -> int:
    """Count running jobs for a specific concurrency key."""
    async with chancy.pool.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                f"""
                SELECT COUNT(*) FROM {chancy.prefix}jobs
                WHERE concurrency_key = %s AND state = 'running'
                """,
                (concurrency_key,),
            )
            result = await cursor.fetchone()
            return result[0] if result else 0


async def _sample_running_counts(
    chancy: Chancy,
    concurrency_key: str,
    samples: int = 20,
    interval: float = 0.25,
) -> list[int]:
    """Sample running job counts over time."""
    counts = []
    for _ in range(samples):
        count = await _count_running_jobs_for_key(chancy, concurrency_key)
        counts.append(count)
        await asyncio.sleep(interval)
    return counts


async def _push_many_collect(chancy: Chancy, jobs: list) -> list:
    """Push many jobs and collect all references from the async generator."""
    refs = []
    async for batch_refs in chancy.push_many(jobs):
        refs.extend(batch_refs)
    return refs


class TestConcurrencyKeyEvaluation:
    """Test concurrency key evaluation logic"""

    def test_no_concurrency_key(self):
        """Test job without concurrency constraints"""
        job = Job.from_func(simple_job)
        result = job.evaluate_concurrency_key()
        assert result is None

    def test_max_concurrent_only(self):
        """Test job with only max_concurrent (no key specified)"""
        job = Job.from_func(simple_job).with_concurrency(ConcurrencyRule(max=3))
        result = job.evaluate_concurrency_key()
        assert result == "test_concurrency.simple_job"

    def test_simple_field_key(self):
        """Test simple field-based concurrency key"""
        job = (
            Job.from_func(simple_job)
            .with_concurrency(ConcurrencyRule(max=1, key="user_id"))
            .with_kwargs(user_id="123", action="upload")
        )
        result = job.evaluate_concurrency_key()
        assert result == "test_concurrency.simple_job:123"

    def test_callable_key(self):
        """Test callable concurrency key"""

        def key_func(user_id: str, action: str, **kw) -> str:
            return f"{user_id}:{action}"

        job = (
            Job.from_func(simple_job)
            .with_concurrency(ConcurrencyRule(max=1, key=key_func))
            .with_kwargs(user_id="123", action="upload")
        )
        result = job.evaluate_concurrency_key()
        assert result == "test_concurrency.simple_job:123:upload"

    def test_missing_field_raises_error(self):
        """Test that missing field raises an error"""
        job = (
            Job.from_func(simple_job)
            .with_concurrency(ConcurrencyRule(max=1, key="missing_field"))
            .with_kwargs(user_id="123")
        )
        with pytest.raises(
            ValueError, match="Failed to evaluate concurrency key"
        ):
            job.evaluate_concurrency_key()

    def test_callable_exception_raises_error(self):
        """Test that callable exceptions are properly raised"""

        def failing_key(**kwargs):
            raise ValueError("Test error")

        job = (
            Job.from_func(simple_job)
            .with_concurrency(ConcurrencyRule(max=1, key=failing_key))
            .with_kwargs(user_id="123")
        )
        with pytest.raises(
            ValueError, match="Failed to evaluate concurrency key"
        ):
            job.evaluate_concurrency_key()

    def test_none_values_from_callable(self):
        """Test that None values from callables raise errors"""

        def none_key(**kwargs):
            return None

        job = (
            Job.from_func(simple_job)
            .with_concurrency(ConcurrencyRule(max=1, key=none_key))
            .with_kwargs(user_id="123")
        )
        with pytest.raises(
            ValueError, match="Failed to evaluate concurrency key"
        ):
            job.evaluate_concurrency_key()


class TestJobWithConcurrency:
    """Test Job class concurrency methods"""

    def test_with_concurrency_method(self):
        """Test the with_concurrency fluent method"""
        # Test simple string key
        job_with_concurrency = simple_job.job.with_concurrency(
            ConcurrencyRule(max=3, key="user_id")
        )
        assert job_with_concurrency.concurrency_rule.key == "user_id"
        assert job_with_concurrency.concurrency_rule.max == 3

        # Original job should be unchanged (immutable)
        assert simple_job.job.concurrency_rule is None

    def test_with_concurrency_callable_key(self):
        """Test with_concurrency with callable key"""

        def key_func(user_id: str, action: str, **kw) -> str:
            return f"{user_id}:{action}"

        job_with_concurrency = simple_job.job.with_concurrency(
            ConcurrencyRule(max=5, key=key_func)
        )
        assert job_with_concurrency.concurrency_rule.key == key_func
        assert job_with_concurrency.concurrency_rule.max == 5


@pytest.mark.asyncio
class TestConcurrencyIntegration:
    """Integration tests for concurrency constraints"""

    async def test_concurrency_config_storage(self, chancy: Chancy):
        """Test that concurrency configurations are stored in the database"""

        # Push a job with concurrency constraints
        job_with_concurrency = slow_job.job.with_concurrency(
            ConcurrencyRule(max=3, key="user_id")
        ).with_kwargs(user_id="user_123", action="test")
        await chancy.push(job_with_concurrency)

        # Check that concurrency config was stored
        async with chancy.pool.connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    f"SELECT * FROM {chancy.prefix}concurrency_configs WHERE concurrency_key = %s",
                    ("test_concurrency.slow_job:user_123",),
                )
                result = await cursor.fetchone()

                assert result is not None
                assert (
                    result[0] == "test_concurrency.slow_job:user_123"
                )  # concurrency_key (prefixed)
                assert result[1] == 3  # concurrency_max

    async def test_basic_concurrency_limiting(
        self, chancy: Chancy, worker: Worker
    ):
        """Test basic concurrency limiting verifies limit is enforced"""
        concurrency_key = "test_concurrency.slow_job:user_123"

        # Create a job with concurrency limit of 2 per user
        job_with_concurrency = slow_job.job.with_concurrency(
            ConcurrencyRule(max=2, key="user_id")
        )

        # Push 5 jobs for the same user - should only run 2 at a time
        jobs = [
            job_with_concurrency.with_kwargs(
                user_id="user_123", action=f"action_{i}", duration=0.5
            )
            for i in range(5)
        ]
        refs = await _push_many_collect(chancy, jobs)

        # Sample running counts while jobs execute
        await asyncio.sleep(0.2)
        running_counts = await _sample_running_counts(
            chancy, concurrency_key, samples=15, interval=0.1
        )

        # Wait for all jobs to complete
        completed_jobs = await chancy.wait_for_jobs(refs, timeout=30)

        # All jobs should be completed
        for job_result in completed_jobs:
            assert job_result.state == QueuedJob.State.SUCCEEDED

        # Verify concurrency limit was respected
        max_observed = max(running_counts) if running_counts else 0
        assert max_observed <= 2, (
            f"Concurrency limit violated: observed {max_observed} "
            f"concurrent jobs, expected at most 2. Samples: {running_counts}"
        )
        # Verify we actually observed some concurrency
        assert max_observed >= 1, (
            f"Expected to observe at least 1 running job. Samples: {running_counts}"
        )

    async def test_concurrency_limit_enforced_across_workers(
        self, chancy: Chancy
    ):
        """
        Test that concurrency limits are enforced across multiple workers.

        This test:
        1. Starts multiple workers
        2. Pushes many jobs with a concurrency limit of 2 for the same key
        3. Samples running job count and verifies it never exceeds the limit
        """
        concurrency_key = "test_concurrency.slow_job:shared_user"

        # Create job with concurrency limit of 2 per user
        job_template = slow_job.job.with_concurrency(
            ConcurrencyRule(max=2, key="user_id")
        )

        # Push 8 jobs for the same user - should only run 2 at a time
        jobs = [
            job_template.with_kwargs(user_id="shared_user", duration=0.5)
            for _ in range(8)
        ]
        refs = await _push_many_collect(chancy, jobs)

        # Start 3 workers to increase parallelism pressure
        workers = [Worker(chancy, shutdown_timeout=30) for _ in range(3)]
        for w in workers:
            await w.start()

        try:
            # Sample running counts while jobs execute
            # Start sampling after a brief delay to let jobs start
            await asyncio.sleep(0.3)
            running_counts = await _sample_running_counts(
                chancy, concurrency_key, samples=30, interval=0.1
            )

            # Wait for all jobs to complete
            completed_jobs = await chancy.wait_for_jobs(refs, timeout=60)

            # Verify all jobs completed successfully
            for job_result in completed_jobs:
                assert job_result.state == QueuedJob.State.SUCCEEDED

            # Verify concurrency limit was respected during sampling
            max_observed = max(running_counts) if running_counts else 0
            assert max_observed <= 2, (
                f"Concurrency limit violated: observed {max_observed} "
                f"concurrent jobs, expected at most 2. Samples: {running_counts}"
            )
            # Verify we actually observed some concurrency
            assert max_observed >= 1, (
                f"Expected to observe at least 1 running job. Samples: {running_counts}"
            )

        finally:
            # Stop all workers
            for w in workers:
                await w.stop()

    async def test_jobs_without_concurrency_not_blocked_by_limited_jobs(
        self, chancy: Chancy, worker: Worker
    ):
        """
        Test that jobs without concurrency constraints are not blocked
        by jobs that have concurrency limits.
        """
        concurrency_key = "test_concurrency.slow_job:limited_user"

        # Push multiple jobs with strict concurrency limit (max 1)
        limited_jobs = [
            slow_job.job.with_concurrency(
                ConcurrencyRule(max=1, key="user_id")
            ).with_kwargs(user_id="limited_user", duration=0.6)
            for _ in range(3)
        ]

        # Push regular jobs without concurrency constraints
        regular_jobs = [simple_job.job for _ in range(3)]

        # Push all jobs - limited jobs first, then regular jobs
        refs = await _push_many_collect(chancy, limited_jobs + regular_jobs)

        # Sample running counts for the limited key
        await asyncio.sleep(0.2)
        running_counts = await _sample_running_counts(
            chancy, concurrency_key, samples=15, interval=0.1
        )

        # Wait for all jobs and verify they succeeded
        completed_jobs = await chancy.wait_for_jobs(refs, timeout=30)
        for job_result in completed_jobs:
            assert job_result.state == QueuedJob.State.SUCCEEDED

        # Verify the limited jobs never exceeded their concurrency limit
        max_observed = max(running_counts) if running_counts else 0
        assert max_observed <= 1, (
            f"Concurrency limit violated: observed {max_observed} "
            f"concurrent jobs, expected at most 1. Samples: {running_counts}"
        )
