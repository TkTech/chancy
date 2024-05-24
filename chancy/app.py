import re
import enum
import dataclasses
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from typing import Optional, Callable

from psycopg import sql
from psycopg import AsyncCursor, Cursor
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool

from chancy.migrate import Migrator
from chancy.utils import importable_name


@dataclass
class JobContext:
    job: "Job"


@dataclass
class Limit:
    """
    Defines a resource limit for the execution of a job.
    """

    class Type(enum.Enum):
        """
        The type of resource limit.
        """

        #: The maximum amount of memory (in bytes) the job can use.
        MEMORY = "memory"
        #: The maximum amount of time (in seconds) the job can run.
        TIME = "time"

    #: The type of limit.
    type: Type
    #: The value of the limit.
    value: int

    @classmethod
    def deserialize(cls, data: dict):
        return cls(
            type=cls.Type(data["type"]),
            value=data["value"],
        )

    def serialize(self) -> dict:
        return {
            "type": self.type.value,
            "value": self.value,
        }


@dataclass(frozen=True, kw_only=True)
class Job:
    """
    Defines a job to be executed by Chancy.
    """

    # The following fields exist only inside the `payload` field in the
    # database.

    #: The import path of the job to execute.
    func: str | Callable[..., None]
    #: The keyword arguments to pass to the job.
    kwargs: Optional[dict] = None
    #: The priority of the job.
    priority: int = 0
    #: Optional resource limits to apply to the job.
    limits: list[Limit] = dataclasses.field(default_factory=list)

    # The following fields exist as columns in the database.

    #: The ID of the job, if it has been created in the database.
    id: Optional[int] = None
    #: The number of times this job has been attempted.
    attempts: Optional[int] = None
    #: The maximum number of times this job should be attempted.
    max_attempts: int = 1
    #: The state of the job.
    state: Optional[str] = None
    #: The time the job was created.
    created_at: Optional[datetime] = None
    #: The time the job was started.
    started_at: Optional[datetime] = None
    #: The time the job was completed.
    completed_at: Optional[datetime] = None
    #: The time the job is scheduled to run.
    scheduled_at: Optional[datetime] = None

    @classmethod
    def deserialize(cls, data: dict):
        return cls(
            func=data["payload"]["name"],
            kwargs=data["payload"]["kwargs"],
            limits=[
                Limit.deserialize(limit) for limit in data["payload"]["limits"]
            ],
            id=data["id"],
            attempts=data["attempts"],
            max_attempts=data["max_attempts"],
            state=data["state"],
            created_at=data["created_at"],
            started_at=data["started_at"],
            completed_at=data["completed_at"],
            scheduled_at=data["scheduled_at"],
        )

    def serialize(self) -> dict:
        func = importable_name(self.func) if callable(self.func) else self.func
        return {
            "id": self.id,
            "payload": {
                "name": func,
                "kwargs": self.kwargs,
                "limits": [limit.serialize() for limit in self.limits],
            },
            "attempts": self.attempts,
            "max_attempts": self.max_attempts,
            "state": self.state,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "scheduled_at": self.scheduled_at,
        }


@dataclasses.dataclass(frozen=True)
class Queue:
    """
    Defines a named queue for processing jobs in Chancy.
    """

    class State(enum.IntEnum):
        """
        The state of a queue.
        """

        #: The queue is currently processing jobs.
        ACTIVE = 1
        #: The queue is paused and not processing jobs.
        PAUSED = 2

    #: A name for the queue, must only be a-z and underscore.
    name: str
    #: Maximum number of jobs to process concurrently (per-worker).
    concurrency: int = 1
    #: The state of the queue.
    state: State = State.ACTIVE
    #: The maximum number of jobs to process before a worker on this queue
    #: should be restarted. Can help with memory leaks.
    maximum_jobs_per_worker: Optional[int] = None
    #: A function to call when a worker process for this queue is first started.
    #: This can be used to do expensive setup operations once per worker, like
    #: setting up django.
    on_setup: Optional[Callable[[], None]] = None


@dataclasses.dataclass
class Chancy:
    """
    The main application object for Chancy.
    """

    #: A valid engine connection string.
    dsn: str
    #: A list of queues that this app should be aware of for processing jobs.
    queues: list[Queue] = dataclasses.field(default_factory=list)
    #: A list of plugins to enable for this application.
    plugins: list[str] = dataclasses.field(default_factory=list)
    #: The prefix to use for all Chancy tables in the database.
    prefix: str = "chancy_"
    #: The minimum number of seconds to wait when polling the queue.
    poller_delay: int = 5
    #: Disable events, relying on polling only.
    disable_events: bool = False

    #: The minimum number of connections to keep in the pool.
    min_pool_size: int = 1
    #: The maximum number of connections to keep in the pool.
    max_pool_size: int = 3

    def __post_init__(self):
        # Ensure all defined queues have distinct names.
        queue_names = [queue.name for queue in self.queues]
        if len(queue_names) != len(set(queue_names)):
            raise ValueError("Duplicate queue names are not allowed.")

        # Ensure all queue names are valid a-zA-Z and underscore.
        for queue_name in queue_names:
            if not re.match(r"^[a-z_]+$", queue_name):
                raise ValueError("Queue names must only contain a-z, and _.")

        if self.poller_delay < 1:
            raise ValueError("poller_delay must be greater than 0.")

        if self.min_pool_size < 1:
            raise ValueError("min_pool_size must be greater than 0.")

    async def migrate(self):
        """
        Migrate the database to the latest schema version.

        This method will apply any pending migrations to the database to bring
        it up to the latest schema version. If the database is already up-to-
        date, this method will do nothing.
        """
        migrator = Migrator("chancy", "chancy.migrations", prefix=self.prefix)
        async with self.pool.connection() as conn:
            await migrator.migrate(conn)

    @cached_property
    def pool(self):
        """
        A connection pool for the Chancy application.
        """
        return AsyncConnectionPool(
            self.dsn,
            open=False,
            check=AsyncConnectionPool.check_connection,
            min_size=self.min_pool_size,
            max_size=self.max_pool_size,
        )

    async def __aenter__(self):
        await self.pool.open()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.pool.close()
        return False

    async def submit(self, job: Job, queue: Queue):
        """
        Submit a job to the specified queue.

        This method will insert the job into the database and notify any
        listening workers that a new job is available. It will be submitted
        immediately using a new connection and transaction. To submit multiple
        jobs in a single transaction, use `submit_to_cursor`.
        """
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                async with conn.transaction():
                    await self.submit_to_cursor(cur, job, queue)

    async def submit_to_cursor(
        self, cursor: AsyncCursor, job: Job, queue: Queue | str
    ):
        """
        Submit a job to the specified queue using the provided cursor.

        This method is useful for submitting multiple jobs in a single
        transaction or for ensuring your job is only inserted when a
        transaction the caller is managing is successful.
        """
        await cursor.execute(
            sql.SQL(
                """
                INSERT INTO {jobs} (
                    queue,
                    priority,
                    max_attempts,
                    payload,
                    scheduled_at
                )
                VALUES (
                    %(name)s,
                    %(priority)s,
                    %(max_attempts)s,
                    %(payload)s,
                    %(scheduled_at)s
                )
                """
            ).format(jobs=sql.Identifier(f"{self.prefix}jobs")),
            {
                "name": queue.name if isinstance(queue, Queue) else queue,
                "payload": Json(job.serialize()["payload"]),
                "priority": job.priority,
                "max_attempts": job.max_attempts,
                "scheduled_at": job.scheduled_at,
            },
        )
        await cursor.execute(
            "SELECT pg_notify(%s, %s::text)",
            [
                f"{self.prefix}events",
                Json(
                    {
                        "event": "job_submitted",
                        "job": job.id,
                        "queue": (
                            queue.name if isinstance(queue, Queue) else queue
                        ),
                    }
                ),
            ],
        )

    def sync_submit_to_cursor(
        self, cursor: Cursor, job: Job, queue: Queue | str
    ):
        """
        Submit a job to the specified queue using the provided (synchronous)
        cursor.

        This method is useful for submitting multiple jobs in a single
        transaction or for ensuring your job is only inserted when a
        transaction the caller is managing is successful.
        """
        cursor.execute(
            sql.SQL(
                """
                INSERT INTO {jobs} (
                    queue,
                    priority,
                    max_attempts,
                    payload
                )
                VALUES (
                    %(name)s,
                    %(priority)s,
                    %(max_attempts)s,
                    %(payload)s
                )
                """
            ).format(jobs=sql.Identifier(f"{self.prefix}jobs")),
            {
                "name": queue.name if isinstance(queue, Queue) else queue,
                "payload": Json(job.serialize()["payload"]),
                "priority": job.priority,
                "max_attempts": job.max_attempts,
            },
        )
        cursor.execute(
            "SELECT pg_notify(%s, %s::text)",
            [
                f"{self.prefix}events",
                Json(
                    {
                        "event": "job_submitted",
                        "job": job.id,
                        "queue": (
                            queue.name if isinstance(queue, Queue) else queue
                        ),
                    }
                ),
            ],
        )
