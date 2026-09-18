"""
Unmanaged SQLAlchemy models that match the Chancy tables.

These models allow you to query Chancy data using SQLAlchemy's ORM without
managing the table schema (Chancy handles migrations).

Example usage::

    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session
    from chancy.contrib.sqlalchemy.models import Job, Queue, Worker

    engine = create_engine("postgresql://...")
    
    with Session(engine) as session:
        # Get all pending jobs
        pending_jobs = session.query(Job).filter(Job.state == "pending").all()
        
        # Get all active queues
        active_queues = session.query(Queue).filter(Queue.state == "active").all()
        
        # Get all workers
        workers = session.query(Worker).all()

You can customize the table prefix by setting ``CHANCY_PREFIX`` before 
importing the models::

    import os
    os.environ["CHANCY_PREFIX"] = "myapp_chancy_"
    
    from chancy.contrib.sqlalchemy.models import Job, Queue, Worker
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Optional
from uuid import UUID

from sqlalchemy import (
    Boolean,
    DateTime,
    Integer,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

__all__ = ("Base", "Job", "Worker", "Queue")

PREFIX = os.environ.get("CHANCY_PREFIX", "chancy_")


class Base(DeclarativeBase):
    """Base class for Chancy SQLAlchemy models."""

    pass


class Job(Base):
    """
    SQLAlchemy model representing a Chancy job.

    Jobs are the core unit of work in Chancy. Each job represents a function
    call that will be executed by a worker.

    Attributes:
        id: Unique identifier for the job (UUID).
        queue: Name of the queue this job belongs to.
        func: Fully qualified name of the function to execute.
        kwargs: JSON object containing keyword arguments for the function.
        limits: JSON array of rate limit configurations.
        meta: JSON object for storing arbitrary metadata.
        state: Current state of the job (pending, running, completed, failed, etc.).
        priority: Job priority (higher values = higher priority).
        attempts: Number of execution attempts made.
        max_attempts: Maximum number of attempts before marking as failed.
        taken_by: Worker ID that is currently processing this job.
        created_at: Timestamp when the job was created.
        started_at: Timestamp when job execution started.
        completed_at: Timestamp when job execution completed.
        scheduled_at: Timestamp when the job is scheduled to run.
        unique_key: Optional key for ensuring job uniqueness.
        errors: JSON array of error information from failed attempts.
    """

    __tablename__ = f"{PREFIX}jobs"
    __table_args__ = {"extend_existing": True}

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
    )
    queue: Mapped[str] = mapped_column(Text, nullable=False)
    func: Mapped[str] = mapped_column(Text, nullable=False)
    kwargs: Mapped[dict[str, Any]] = mapped_column(
        JSONB, server_default="{}", nullable=False
    )
    limits: Mapped[list[Any]] = mapped_column(
        JSONB, server_default="[]", nullable=False
    )
    meta: Mapped[dict[str, Any]] = mapped_column(
        JSONB, server_default="{}", nullable=False
    )
    state: Mapped[str] = mapped_column(
        Text, nullable=False, default="pending"
    )
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1
    )
    taken_by: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    started_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    scheduled_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    unique_key: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    errors: Mapped[list[Any]] = mapped_column(
        JSONB, server_default="[]", nullable=False
    )

    def __repr__(self) -> str:
        return (
            f"<Job(id={self.id!r}, queue={self.queue!r}, "
            f"func={self.func!r}, state={self.state!r})>"
        )


class Worker(Base):
    """
    SQLAlchemy model representing a Chancy worker.

    Workers are the processes that execute jobs. Each worker registers itself
    in the database and periodically updates its last_seen timestamp.

    Attributes:
        worker_id: Unique identifier for the worker.
        tags: Array of tags associated with this worker.
        queues: Array of queue names this worker is processing.
        last_seen: Timestamp of the last heartbeat from this worker.
        expires_at: Timestamp when this worker registration expires.
    """

    __tablename__ = f"{PREFIX}workers"
    __table_args__ = {"extend_existing": True}

    worker_id: Mapped[str] = mapped_column(Text, primary_key=True)
    tags: Mapped[list[str]] = mapped_column(
        ARRAY(Text), server_default="{}", nullable=False
    )
    queues: Mapped[list[str]] = mapped_column(
        ARRAY(Text), server_default="{}", nullable=False
    )
    last_seen: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    def __repr__(self) -> str:
        return f"<Worker(worker_id={self.worker_id!r}, queues={self.queues!r})>"


class Queue(Base):
    """
    SQLAlchemy model representing a Chancy queue.

    Queues define how jobs are processed, including concurrency settings,
    rate limits, and the executor to use.

    Attributes:
        name: Unique name for the queue.
        state: Current state of the queue (active, paused, etc.).
        concurrency: Maximum number of concurrent jobs.
        tags: Array of tags for worker targeting.
        executor: Fully qualified name of the executor class.
        executor_options: JSON object of executor-specific options.
        polling_interval: Interval in seconds between job polls.
        rate_limit: Maximum number of jobs per rate_limit_window.
        rate_limit_window: Time window in seconds for rate limiting.
        eager_polling: Whether to poll immediately after job completion.
        created_at: Timestamp when the queue was created.
    """

    __tablename__ = f"{PREFIX}queues"
    __table_args__ = {"extend_existing": True}

    name: Mapped[str] = mapped_column(Text, primary_key=True)
    state: Mapped[str] = mapped_column(Text, nullable=False, default="active")
    concurrency: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    tags: Mapped[list[str]] = mapped_column(
        ARRAY(Text), server_default="{}", nullable=False
    )
    executor: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="chancy.executors.process.ProcessExecutor",
    )
    executor_options: Mapped[dict[str, Any]] = mapped_column(
        JSONB, server_default="{}", nullable=False
    )
    polling_interval: Mapped[int] = mapped_column(
        Integer, nullable=False, default=5
    )
    rate_limit: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    rate_limit_window: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True
    )
    eager_polling: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default="false"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    def __repr__(self) -> str:
        return f"<Queue(name={self.name!r}, state={self.state!r})>"
