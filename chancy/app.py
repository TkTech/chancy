import re
import enum
import dataclasses
from datetime import datetime
from functools import cached_property
from typing import Optional, Callable

from psycopg import sql
from psycopg import AsyncCursor, Cursor
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool

from chancy.migrate import Migrator
from chancy.utils import importable_name


@dataclasses.dataclass(frozen=True)
class JobContext:
    """
    A context object that can be requested by a running job to get information
    about the job itself.
    """

    #: The original job that was executed.
    job: "Job"
    #: The ID of the job, if it has been created in the database.
    id: Optional[int] = None
    #: The number of times this job has been attempted.
    attempts: Optional[int] = None
    #: The state of the job.
    state: Optional[str] = None
    #: The time the job was created.
    created_at: Optional[datetime] = None
    #: The time the job was started.
    started_at: Optional[datetime] = None
    #: The time the job was completed.
    completed_at: Optional[datetime] = None


@dataclasses.dataclass
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
        return cls(type=cls.Type(data["t"]), value=data["v"])

    def serialize(self) -> dict:
        return {"t": self.type.value, "v": self.value}


@dataclasses.dataclass(frozen=True, kw_only=True)
class Job:
    """
    Defines a job to be executed by Chancy.

    Once created, a Job is immutable and should be treated as such. Use the
    `with_` methods to create a new job with updated values.

    Example:

    .. code-block:: python

        job = Job(func=my_task, kwargs={"foo": "bar"})
        job = job.with_priority(10)
    """

    #: A function or importable name to call when the job is executed.
    func: str | Callable[..., None]
    #: The keyword arguments to pass to the job.
    kwargs: Optional[dict] = None
    #: The priority of the job. Lower numbers are executed first, with 0
    #: being the highest priority.
    priority: int = 0
    #: Optional resource limits to apply to the job.
    limits: list[Limit] = dataclasses.field(default_factory=list)
    #: The maximum number of times this job should be attempted.
    max_attempts: int = 1
    #: The time the job is scheduled to run.
    scheduled_at: Optional[datetime] = None

    def __post_init__(self):
        if self.priority < 0:
            raise ValueError("Priority must be greater than or equal to 0.")

        if self.max_attempts < 1:
            raise ValueError("max_attempts must be greater than 0.")

    def with_kwargs(self, **kwargs) -> "Job":
        """
        Create a new job with updated keyword arguments.
        """
        return dataclasses.replace(self, kwargs=kwargs)

    def with_priority(self, priority) -> "Job":
        """
        Create a new job with an updated priority.
        """
        return dataclasses.replace(self, priority=priority)

    def with_limits(self, limits) -> "Job":
        """
        Create a new job with updated limits.
        """
        return dataclasses.replace(self, limits=limits)

    def with_max_attempts(self, max_attempts) -> "Job":
        """
        Create a new job with an updated maximum number of attempts.
        """
        return dataclasses.replace(self, max_attempts=max_attempts)


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
    #: The state of the queue. By default, queues will start as active.
    state: State = State.ACTIVE
    #: The maximum number of jobs to process before a worker on this queue
    #: should be restarted. Useful for dealing with unavoidable memory leaks.
    maximum_jobs_per_worker: Optional[int] = None
    #: A function to call when a worker process for this queue is first started.
    #: This can be used to do expensive setup operations once per worker, like
    #: setting up django.
    on_setup: Optional[Callable[[], None]] = None


def _prepare_submit(
    app: "Chancy", queue: Queue, job: Job
) -> tuple[sql.Composed, dict]:
    f = importable_name(job.func) if callable(job.func) else job.func

    return (
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
        ).format(jobs=sql.Identifier(f"{app.prefix}jobs")),
        {
            "name": queue.name if isinstance(queue, Queue) else queue,
            "payload": Json(
                {
                    "func": f,
                    "kwargs": job.kwargs,
                    "limits": [limit.serialize() for limit in job.limits],
                }
            ),
            "priority": job.priority,
            "max_attempts": job.max_attempts,
            "scheduled_at": job.scheduled_at,
        },
    )


@dataclasses.dataclass
class Chancy:
    """
    The main application object for Chancy containing all configuration and
    helpers for common operations such as submitting jobs and performing a
    migration.

    The application object is an async context manager and should be used
    within an `async with` block to ensure the connection pool is properly
    opened and closed.

    Example:

    .. code-block:: python

        async with Chancy(
            dsn="postgresql://postgres:localtest@localhost:8190/postgres",
            queues=[
                Queue(name="default", concurrency=1),
            ],
        ) as app:
            ...

    If being used in a non-async application to submit jobs, you can use the
    `sync_submit_to_cursor` method to submit jobs synchronously using your
    own cursor & connection.

    Example:

    .. code-block:: python

        with connection() as conn:
            with conn.cursor() as cur:
                app.sync_submit_to_cursor(
                    cur,
                    Job(func=my_dummy_task),
                    "default"
                )

    """

    #: A postgres DSN to connect to the database.
    dsn: str
    #: A list of queues that this app should be aware of for processing jobs.
    queues: list[Queue] = dataclasses.field(default_factory=list)
    #: A list of plugins to enable for this application.
    plugins: list[str] = dataclasses.field(default_factory=list)
    #: The prefix to use for all Chancy tables in the database.
    prefix: str = "chancy_"
    #: The minimum number of seconds to wait between polling for new jobs.
    job_poller_interval: int = 5
    #: The minimum number of seconds to wait between attempts to acquire
    #: leadership of the cluster.
    leadership_poller_interval: int = 30
    #: The number of seconds before a leader is considered expired.
    leadership_timeout: int = 60
    #: Disable pub/sub events, relying on periodic polling only.
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

        if self.job_poller_interval < 1:
            raise ValueError("poller_delay must be greater than 0.")

        if self.min_pool_size < 1:
            raise ValueError("min_pool_size must be greater than 0.")

    async def migrate(self, *, to_version: int | None = None):
        """
        Migrate the database to the latest schema version.

        This method will apply any pending migrations to the database to bring
        it up to the latest schema version. If the database is already up-to-
        date, this method will do nothing.

        If `to_version` is provided, the database will be migrated up/down
        to the specified version.

        .. code-block:: python

            async with Chancy(
                dsn="postgresql://username:password@localhost:8190/postgres",
            ) as app:
                await app.migrate()
        """
        migrator = Migrator("chancy", "chancy.migrations", prefix=self.prefix)
        async with self.pool.connection() as conn:
            return await migrator.migrate(conn, to_version=to_version)

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
        query, params = _prepare_submit(self, queue, job)
        await cursor.execute(query, params)
        await cursor.execute(
            "SELECT pg_notify(%s, %s::text)",
            [
                f"{self.prefix}events",
                Json(
                    {
                        "event": "job_submitted",
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
        query, params = _prepare_submit(self, queue, job)
        cursor.execute(query, params)
        cursor.execute(
            "SELECT pg_notify(%s, %s::text)",
            [
                f"{self.prefix}events",
                Json(
                    {
                        "event": "job_submitted",
                        "queue": (
                            queue.name if isinstance(queue, Queue) else queue
                        ),
                    }
                ),
            ],
        )
