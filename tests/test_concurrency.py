import time

import pytest

from chancy import Chancy, Job, Queue, QueuedJob, Worker, job


@job()
def simple_job():
    """A simple job for testing"""
    pass


@job()
def user_job(user_id: str, action: str):
    """A job that operates on a specific user"""
    time.sleep(0.1)  # Simulate some work


class TestConcurrencyKeyEvaluation:
    """Test concurrency key evaluation logic"""

    def test_no_concurrency_key(self):
        """Test job without concurrency constraints"""
        job = Job.from_func(simple_job)
        result = job.evaluate_concurrency_key()
        assert result is None

    def test_max_concurrent_only(self):
        """Test job with only max_concurrent (no key specified)"""
        job = Job.from_func(simple_job).with_concurrency(3)
        result = job.evaluate_concurrency_key()
        assert result == "test_concurrency.simple_job"

    def test_simple_field_key(self):
        """Test simple field-based concurrency key"""
        job = (
            Job.from_func(simple_job)
            .with_concurrency(1, "user_id")
            .with_kwargs(user_id="123", action="upload")
        )
        result = job.evaluate_concurrency_key()
        assert result == "test_concurrency.simple_job:123"

    def test_callable_key(self):
        """Test callable concurrency key"""
        key_func = lambda user_id, action, **kw: f"{user_id}:{action}"
        job = (
            Job.from_func(simple_job)
            .with_concurrency(1, key_func)
            .with_kwargs(user_id="123", action="upload")
        )
        result = job.evaluate_concurrency_key()
        assert result == "test_concurrency.simple_job:123:upload"

    def test_missing_field_raises_error(self):
        """Test that missing field raises an error"""
        job = (
            Job.from_func(simple_job)
            .with_concurrency(1, "missing_field")
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
            .with_concurrency(1, failing_key)
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
            .with_concurrency(1, none_key)
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
        job_with_concurrency = simple_job.job.with_concurrency(3, "user_id")
        assert job_with_concurrency.concurrency_key == "user_id"
        assert job_with_concurrency.concurrency_max == 3

        # Original job should be unchanged (immutable)
        assert simple_job.job.concurrency_key is None
        assert simple_job.job.concurrency_max is None

    def test_with_concurrency_callable_key(self):
        """Test with_concurrency with callable key"""
        key_func = lambda user_id, action, **kw: f"{user_id}:{action}"

        job_with_concurrency = simple_job.job.with_concurrency(5, key_func)
        assert job_with_concurrency.concurrency_key == key_func
        assert job_with_concurrency.concurrency_max == 5


@pytest.mark.asyncio
class TestConcurrencyIntegration:
    """Integration tests for concurrency constraints"""

    async def test_basic_concurrency_limiting(
        self, chancy: Chancy, worker: Worker
    ):
        """Test basic concurrency limiting with multiple workers"""
        await chancy.declare(Queue("default"))

        # Create a job with concurrency limit of 1 per user
        job_with_concurrency = user_job.job.with_concurrency(1, "user_id")

        # Push 3 jobs for the same user
        refs = []
        for i in range(3):
            job_instance = job_with_concurrency.with_kwargs(
                user_id="user_123", action=f"action_{i}"
            )
            ref = await chancy.push(job_instance)
            refs.append(ref)

        # Wait for at least one job to complete
        await chancy.wait_for_job(refs[0], timeout=30)

        # Check that jobs were processed with concurrency constraints
        completed_jobs = []
        for ref in refs:
            job = await chancy.get_job(ref)
            if job and job.state == QueuedJob.State.SUCCEEDED:
                completed_jobs.append(job)

        # At least one job should be completed
        assert len(completed_jobs) >= 1

    async def test_different_concurrency_keys_dont_interfere(
        self, chancy: Chancy, worker: Worker
    ):
        """Test that jobs with different concurrency keys don't interfere with each other"""
        await chancy.declare(Queue("default"))

        # Create jobs with concurrency limit of 1 per user
        job_with_concurrency = user_job.job.with_concurrency(1, "user_id")

        # Push jobs for different users
        job1 = job_with_concurrency.with_kwargs(
            user_id="user_123", action="action_1"
        )
        job2 = job_with_concurrency.with_kwargs(
            user_id="user_456", action="action_2"
        )
        ref1 = await chancy.push(job1)
        ref2 = await chancy.push(job2)

        # Both jobs should be able to run concurrently since they have different keys
        # Wait for both jobs to complete
        job1 = await chancy.wait_for_job(ref1, timeout=30)
        job2 = await chancy.wait_for_job(ref2, timeout=30)

        # Both jobs should be completed since they have different concurrency keys
        assert job1.state == QueuedJob.State.SUCCEEDED
        assert job2.state == QueuedJob.State.SUCCEEDED

    async def test_jobs_without_concurrency_unaffected(
        self, chancy: Chancy, worker: Worker
    ):
        """Test that jobs without concurrency constraints work as before"""
        await chancy.declare(Queue("default"))

        # Push regular jobs without concurrency constraints
        refs = []
        for i in range(3):
            ref = await chancy.push(simple_job.job)
            refs.append(ref)

        # Wait for all jobs to complete
        for ref in refs:
            job = await chancy.wait_for_job(ref, timeout=30)
            assert job.state == QueuedJob.State.SUCCEEDED

    async def test_callable_concurrency_key_integration(
        self, chancy: Chancy, worker: Worker
    ):
        """Test integration with callable concurrency keys"""
        await chancy.declare(Queue("default"))

        # Create job with callable concurrency key
        key_func = lambda user_id, action, **kw: f"{user_id}:{action}"
        job_with_concurrency = user_job.job.with_concurrency(1, key_func)

        # Push jobs with same composite key
        job1 = job_with_concurrency.with_kwargs(
            user_id="user_123", action="upload"
        )
        job2 = job_with_concurrency.with_kwargs(
            user_id="user_123", action="upload"
        )
        job3 = job_with_concurrency.with_kwargs(
            user_id="user_123", action="download"
        )
        ref1 = await chancy.push(job1)
        ref2 = await chancy.push(job2)
        ref3 = await chancy.push(job3)

        # Wait for at least one job to complete
        await chancy.wait_for_job(ref1, timeout=30)

        # Jobs 1 and 2 should be limited by concurrency (same key: "user_123:upload")
        # Job 3 should run independently (different key: "user_123:download")
        job1 = await chancy.get_job(ref1)
        job2 = await chancy.get_job(ref2)
        job3 = await chancy.get_job(ref3)

        # At least job1 should be completed
        assert job1.state == QueuedJob.State.SUCCEEDED

        # Job3 should also complete since it has a different concurrency key
        assert job3.state in [
            QueuedJob.State.RUNNING,
            QueuedJob.State.SUCCEEDED,
        ]

    async def test_concurrency_config_storage(self, chancy: Chancy):
        """Test that concurrency configurations are stored in the database"""
        await chancy.migrate()

        # Push a job with concurrency constraints
        job_with_concurrency = user_job.job.with_concurrency(
            3, "user_id"
        ).with_kwargs(user_id="user_123", action="test")
        await chancy.push(job_with_concurrency)

        # Check that concurrency config was stored
        async with chancy.pool.connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    f"SELECT * FROM {chancy.prefix}concurrency_configs WHERE concurrency_key = %s",
                    ("test_concurrency.user_job:user_123",),
                )
                result = await cursor.fetchone()

                assert result is not None
                assert (
                    result[0] == "test_concurrency.user_job:user_123"
                )  # concurrency_key
                assert result[1] == 3  # concurrency_max


@pytest.mark.asyncio
class TestConcurrencyEdgeCases:
    """Test edge cases and error conditions"""

    async def test_concurrency_with_high_limits(
        self, chancy: Chancy, worker: Worker
    ):
        """Test concurrency with limits higher than job count"""
        await chancy.declare(Queue("default"))

        # Create job with high concurrency limit
        job_with_concurrency = user_job.job.with_concurrency(100, "user_id")

        # Push only 2 jobs
        job1 = job_with_concurrency.with_kwargs(
            user_id="user_123", action="action_1"
        )
        job2 = job_with_concurrency.with_kwargs(
            user_id="user_123", action="action_2"
        )
        ref1 = await chancy.push(job1)
        ref2 = await chancy.push(job2)

        # Both jobs should be processed since limit is much higher than job count
        job1 = await chancy.wait_for_job(ref1, timeout=30)
        job2 = await chancy.wait_for_job(ref2, timeout=30)

        assert job1.state == QueuedJob.State.SUCCEEDED
        assert job2.state == QueuedJob.State.SUCCEEDED

    async def test_concurrency_config_updates(self, chancy: Chancy):
        """Test that concurrency config updates work correctly"""
        await chancy.migrate()

        # Push job with initial concurrency config
        job_v1 = user_job.job.with_concurrency(1, "user_id").with_kwargs(
            user_id="user_123", action="test"
        )
        await chancy.push(job_v1)

        # Push job with updated concurrency config
        job_v2 = user_job.job.with_concurrency(5, "user_id").with_kwargs(
            user_id="user_456", action="test"
        )
        await chancy.push(job_v2)

        # Check that config was updated
        async with chancy.pool.connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    f"SELECT concurrency_max FROM {chancy.prefix}concurrency_configs WHERE concurrency_key = %s",
                    ("test_concurrency.user_job:user_456",),
                )
                result = await cursor.fetchone()

                # Should have the latest config (5, not 1)
                assert result[0] == 5
