"""
Tests for SQLAlchemy models integration with Chancy.
"""

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from chancy.contrib.sqlalchemy.models import Base, Job, Queue, Worker


@pytest.fixture
def engine():
    """Create a SQLAlchemy engine connected to the test database."""
    return create_engine(
        "postgresql://postgres:localtest@localhost:8190/postgres"
    )


@pytest.fixture
def session(engine):
    """Create a SQLAlchemy session."""
    with Session(engine) as session:
        yield session


class TestSQLAlchemyModels:
    """Test suite for SQLAlchemy model integration."""

    @pytest.mark.asyncio
    async def test_query_queues(self, chancy, session):
        """Test querying queues using SQLAlchemy."""
        # Create a queue using Chancy
        await chancy.push_queue(
            chancy.Queue(
                name="test-sqlalchemy-queue",
                concurrency=5,
                polling_interval=10,
            )
        )

        # Query using SQLAlchemy
        result = session.execute(
            select(Queue).where(Queue.name == "test-sqlalchemy-queue")
        )
        queue = result.scalar_one_or_none()

        assert queue is not None
        assert queue.name == "test-sqlalchemy-queue"
        assert queue.concurrency == 5
        assert queue.polling_interval == 10
        assert queue.state == "active"

    @pytest.mark.asyncio
    async def test_query_jobs(self, chancy, session):
        """Test querying jobs using SQLAlchemy."""
        # Ensure queue exists
        await chancy.push_queue(
            chancy.Queue(
                name="test-job-queue",
                concurrency=1,
            )
        )

        # Create a job using Chancy
        job = await chancy.push(
            chancy.Job.from_func(
                "tests.contrib.sqlalchemy.test_models.dummy_job",
                queue="test-job-queue",
                kwargs={"message": "hello"},
            )
        )

        # Query using SQLAlchemy
        result = session.execute(select(Job).where(Job.id == job.id))
        db_job = result.scalar_one_or_none()

        assert db_job is not None
        assert db_job.id == job.id
        assert db_job.queue == "test-job-queue"
        assert db_job.state == "pending"
        assert db_job.kwargs == {"message": "hello"}

    @pytest.mark.asyncio
    async def test_query_pending_jobs(self, chancy, session):
        """Test filtering jobs by state using SQLAlchemy."""
        # Ensure queue exists
        await chancy.push_queue(
            chancy.Queue(
                name="test-filter-queue",
                concurrency=1,
            )
        )

        # Create multiple jobs
        for i in range(3):
            await chancy.push(
                chancy.Job.from_func(
                    "tests.contrib.sqlalchemy.test_models.dummy_job",
                    queue="test-filter-queue",
                    kwargs={"index": i},
                )
            )

        # Query pending jobs using SQLAlchemy
        result = session.execute(
            select(Job).where(
                Job.queue == "test-filter-queue", Job.state == "pending"
            )
        )
        pending_jobs = result.scalars().all()

        assert len(pending_jobs) == 3

    @pytest.mark.asyncio
    async def test_job_repr(self, chancy, session):
        """Test Job model __repr__ method."""
        await chancy.push_queue(
            chancy.Queue(name="repr-test-queue", concurrency=1)
        )

        job = await chancy.push(
            chancy.Job.from_func(
                "tests.contrib.sqlalchemy.test_models.dummy_job",
                queue="repr-test-queue",
            )
        )

        result = session.execute(select(Job).where(Job.id == job.id))
        db_job = result.scalar_one()

        repr_str = repr(db_job)
        assert "Job" in repr_str
        assert "repr-test-queue" in repr_str

    @pytest.mark.asyncio
    async def test_queue_repr(self, chancy, session):
        """Test Queue model __repr__ method."""
        await chancy.push_queue(
            chancy.Queue(name="repr-queue", concurrency=1)
        )

        result = session.execute(
            select(Queue).where(Queue.name == "repr-queue")
        )
        queue = result.scalar_one()

        repr_str = repr(queue)
        assert "Queue" in repr_str
        assert "repr-queue" in repr_str

    def test_model_table_names(self):
        """Test that model table names have correct prefix."""
        assert Job.__tablename__ == "chancy_jobs"
        assert Worker.__tablename__ == "chancy_workers"
        assert Queue.__tablename__ == "chancy_queues"


def dummy_job(message: str = "", index: int = 0):
    """Dummy job function for testing."""
    return f"processed: {message} {index}"
