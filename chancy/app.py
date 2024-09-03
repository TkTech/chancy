import asyncio
import logging
import time
from abc import abstractmethod, ABC
from typing import Any, Iterator
from functools import cached_property, cache

from psycopg import sql, Cursor, AsyncCursor
from psycopg.cursor import BaseCursor
from psycopg.rows import dict_row
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool, ConnectionPool

from chancy.migrate import Migrator
from chancy.queue import Queue
from chancy.job import Reference, Job, JobInstance
from chancy.plugin import Plugin
from chancy.utils import chancy_uuid, chunked, json_dumps


@cache
def _setup_default_logger():
    logger = logging.getLogger("chancy")
    logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s • %(levelname)s • %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )

    logger.addHandler(handler)
    return logger


class BaseChancy(ABC):
    """
    The base class for the Chancy application.

    Don't use this directly - instead, use the :py:class:`Chancy` or
    :py:class:`SyncChancy` class depending on whether you need an asyncio
    compatible interface or not.

    :param dsn: The DSN to connect to the database.
    :param plugins: The plugins to use with the application.
    :param prefix: A prefix appended to all table names, which can be used to
        namespace the tables for multiple applications or tenants.
    :param min_connection_pool_size: The minimum number of connections to keep
        in the pool.
    :param max_connection_pool_size: The maximum number of connections to keep
        in the pool.
    :param poll_reconnect_timeout: The number of seconds to wait before
        attempting to reconnect to the database if a connection is lost.
    :param notifications: Enables or disables emitting notifications using
        postgres's NOTIFY/LISTEN feature.
    :param log: The logger to use for all application logging.
    """

    def __init__(
        self,
        dsn: str,
        *,
        plugins: list[Plugin] = None,
        prefix: str = "chancy_",
        min_connection_pool_size: int = 1,
        max_connection_pool_size: int = 10,
        poll_reconnect_timeout: int = 60 * 5,
        notifications: bool = True,
        log: logging.Logger | None = None,
    ):
        #: The DSN to connect to the database.
        self.dsn = dsn
        #: The plugins to use with the application.
        self.plugins = plugins or []
        #: A prefix appended to all table names, which can be used to
        #: namespace the tables for multiple applications or tenants.
        self.prefix = prefix
        #: The minimum number of connections to keep in the pool.
        self.min_connection_pool_size = min_connection_pool_size
        #: The maximum number of connections to keep in the pool.
        self.max_connection_pool_size = max_connection_pool_size
        #: The number of seconds to wait before attempting to reconnect to the
        #: database if a connection is lost.
        self.poll_reconnect_timeout = poll_reconnect_timeout
        #: Enables or disables emitting notifications using postgres's
        #: NOTIFY/LISTEN feature.
        self.notifications = notifications
        #: The logger to use for all application logging.
        self.log = log or _setup_default_logger()

    @abstractmethod
    def migrate(self, *, to_version: int | None = None):
        """
        Migrate the database to the latest version.

        This will migrate the core database tables as well as any enabled
        plugins. If a version is provided, the database will be migrated up
        _or_ down to that version as necessary.

        .. code-block:: python

            async with Chancy("postgresql://localhost/chancy") as chancy:
                await chancy.migrate()

        :param to_version: The version to migrate to. If not provided, the
            database will be migrated to the latest version.
        """

    @abstractmethod
    def declare(self, queue: Queue, *, upsert: bool = False) -> Queue:
        """
        Declare a queue in the database.

        Queues in Chancy are the primary mechanism for organizing and
        executing jobs. They can be configured with various options to
        control how jobs are processed, including the concurrency level,
        rate limiting, and tags to restrict which workers can process jobs
        from the queue.

        Queues exist globally, configured at runtime via the database and
        can be updated, reassigned, or deleted at any time without requiring
        a restart of the workers.

        This will create a new queue in the database with the provided
        configuration. If the queue already exists, no changes will be
        made unless the `upsert` parameter is also set to `True`.

        .. code-block:: python

            async with Chancy("postgresql://localhost/chancy") as chancy:
                await chancy.declare(Queue("default"))

        :param queue: The queue to declare.
        :param upsert: If `True`, the queue will be updated if it already
            exists. Defaults to `False`.
        :return: The queue as it exists in the database.
        """

    @abstractmethod
    def push(self, job: Job) -> Reference:
        """
        Push a job onto the queue.

        This method will push a job onto the queue, making it available for
        processing by workers. A :class:`Reference` object is returned that
        can be used to track the progress of the job and retrieve the result
        when it is complete.

        .. code-block:: python

            async with Chancy("postgresql://localhost/chancy") as chancy:
                await chancy.push(Job.from_func(my_job))

        :param job: The job to push onto the queue.
        :return: A reference to the job in the queue.
        """

    @abstractmethod
    def push_many(
        self, jobs: list[Job], *, batch_size: int = 1000
    ) -> Iterator[list[Reference]]:
        """
        Push multiple jobs onto the queue.

        This method will push multiple jobs onto the queue, making them
        available for processing by workers. Jobs are committed in batches,
        with the size of each batch controlled by the `batch_size` parameter.
        Each batch is committed within its own transaction to limit peak
        memory usage.

        Returns an iterator of lists of references to the jobs in the queue,
        one list per batch.

        :param jobs: The jobs to push onto the queue.
        :param batch_size: The number of jobs to push in each batch. Defaults
            to 1000.
        :return: An iterator of lists of references to the jobs in the queue.
        """

    @abstractmethod
    def push_many_ex(
        self, cursor: BaseCursor, jobs: list[Job]
    ) -> list[Reference]:
        """
        Push multiple jobs onto the queue using a specific cursor.

        This is a low-level method that allows for more control over the
        database connection and transaction management. It is recommended to
        use the higher-level `push_many` method in most cases.

        The main advantage of using this function is to ensure your jobs are
        only pushed if some other operation is successful. For example, you
        only want to send an email confirmation if account creation actually
        succeeds.

        :param cursor: The cursor to use for the operation.
        :param jobs: The jobs to push onto the queue.
        :return: A list of references to the jobs in the queue.
        """

    @abstractmethod
    def declare_ex(
        self, cursor: BaseCursor, queue: Queue, *, upsert: bool = False
    ) -> Queue:
        """
        Declare a queue in the database using a specific cursor.

        This is a low-level method that allows for more control over the
        database connection and transaction management. It is recommended to
        use the higher-level `declare` method in most cases.
        """

    @abstractmethod
    def get_job(self, ref: Reference) -> JobInstance | None:
        """
        Resolve a reference to a job instance.

        If the job no longer exists, returns ``None``.
        """

    @abstractmethod
    def wait_for_job(
        self, ref: Reference, *, interval: int = 1
    ) -> JobInstance | None:
        """
        Wait for a job to complete.

        This method will loop until the job referenced by the provided
        reference has completed. The interval parameter controls how often
        the job status is checked.

        If the job no longer exists, returns ``None``.
        """

    @abstractmethod
    def get_all_queues(self) -> list[Queue]:
        """
        Get all queues known to the cluster, regardless of their status
        or if they're assigned to any workers.
        """

    @abstractmethod
    def get_queue(self, name: str) -> Queue:
        """
        Get a specific queue by name.

        :param name: The name of the queue to retrieve.
        """

    def _get_queue_sql(self):
        return sql.SQL(
            """
            SELECT
                *
            FROM
                {queues}
            WHERE
                name = %s
            """
        ).format(queues=sql.Identifier(f"{self.prefix}queues"))

    def _get_all_queues_sql(self):
        return sql.SQL(
            """
            SELECT
                *
            FROM
                {queues}
            """
        ).format(queues=sql.Identifier(f"{self.prefix}queues"))

    def _push_job_sql(self):
        return sql.SQL(
            """
            INSERT INTO
                {jobs} (
                    id,
                    queue,
                    payload,
                    priority,
                    max_attempts,
                    scheduled_at,
                    unique_key
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (unique_key)
            WHERE
                unique_key IS NOT NULL
                    AND state NOT IN ('succeeded', 'failed')
            DO UPDATE
               SET
                   state = EXCLUDED.state
            RETURNING id;
            """
        ).format(jobs=sql.Identifier(f"{self.prefix}jobs"))

    def _get_job_sql(self):
        return sql.SQL(
            """
            SELECT
                *
            FROM
                {jobs}
            WHERE
                id = %s
            """
        ).format(jobs=sql.Identifier(f"{self.prefix}jobs"))

    def _declare_sql(self, upsert: bool):
        action = sql.SQL(
            """
            UPDATE SET
                name = EXCLUDED.name
            """
        )
        if upsert:
            action = sql.SQL(
                """
                UPDATE SET
                    state = EXCLUDED.state,
                    concurrency = EXCLUDED.concurrency,
                    tags = EXCLUDED.tags,
                    executor = EXCLUDED.executor,
                    executor_options = EXCLUDED.executor_options,
                    polling_interval = EXCLUDED.polling_interval,
                    rate_limit = EXCLUDED.rate_limit,
                    rate_limit_window = EXCLUDED.rate_limit_window
                """
            )

        return sql.SQL(
            """
            INSERT INTO {queues} (
                name,
                state,
                concurrency,
                tags,
                executor,
                executor_options,
                polling_interval,
                rate_limit,
                rate_limit_window
            ) VALUES (
                %(name)s,
                %(state)s,
                %(concurrency)s,
                %(tags)s,
                %(executor)s,
                %(executor_options)s,
                %(polling_interval)s,
                %(rate_limit)s,
                %(rate_limit_window)s
            )
            ON CONFLICT (name) DO
                {action}
            RETURNING 
                state,
                concurrency,
                tags,
                polling_interval,
                executor,
                executor_options,
                rate_limit,
                rate_limit_window;
            """
        ).format(
            queues=sql.Identifier(f"{self.prefix}queues"),
            action=action,
        )


class SyncChancy(BaseChancy):
    """
    A synchronous version of the Chancy application.

    This class provides a synchronous interface to the Chancy application, which
    can be used in non-asyncio codebases or for pushing jobs from synchronous
    code. It is recommended to use the async version of the Chancy application
    whenever possible.

    .. code-block:: python

        from chancy import SyncChancy, Job, Queue

        with SyncChancy("postgresql://localhost/chancy") as chancy:
            chancy.migrate()
            chancy.declare(Queue("default"))

    """

    @cached_property
    def pool(self):
        return ConnectionPool(
            self.dsn,
            min_size=self.min_connection_pool_size,
            max_size=self.max_connection_pool_size,
            reconnect_timeout=self.poll_reconnect_timeout,
        )

    def __enter__(self):
        self.pool.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.close()
        return False

    def migrate(self, *, to_version: int | None = None):
        migrator = Migrator("chancy", "chancy.migrations", prefix=self.prefix)
        with self.pool.connection() as conn:
            migrator.migrate(conn, to_version=to_version)

        for plugin in self.plugins:
            plugin.migrate_sync(self, to_version=to_version)

    def declare(self, queue: Queue, *, upsert: bool = False) -> Queue:
        with self.pool.connection() as conn:
            with conn.cursor() as cursor:
                return self.declare_ex(cursor, queue, upsert=upsert)

    def declare_ex(
        self, cursor: Cursor, queue: Queue, *, upsert: bool = False
    ) -> Queue:
        cursor.execute(
            self._declare_sql(upsert),
            {
                "name": queue.name,
                "state": queue.state.value,
                "concurrency": queue.concurrency,
                "tags": list(queue.tags),
                "executor": queue.executor,
                "executor_options": Json(queue.executor_options),
                "polling_interval": queue.polling_interval,
                "rate_limit": queue.rate_limit,
                "rate_limit_window": queue.rate_limit_window,
            },
        )

        result = cursor.fetchone()
        return Queue(
            name=queue.name,
            state=result[0],
            concurrency=result[1],
            tags=set(result[2]),
            polling_interval=result[3],
            executor=result[4],
            executor_options=result[5],
            rate_limit=result[6],
            rate_limit_window=result[7],
        )

    def push(self, job: Job) -> Reference:
        with self.pool.connection() as conn:
            with conn.cursor() as cursor:
                return self.push_many_ex(cursor, [job])[0]

    def push_many(
        self, jobs: list[Job], *, batch_size: int = 1000
    ) -> Iterator[list[Reference]]:
        with self.pool.connection() as conn:
            with conn.cursor() as cursor:
                for chunk in chunked(jobs, batch_size):
                    with conn.transaction():
                        yield self.push_many_ex(cursor, chunk)

    def push_many_ex(self, cursor: Cursor, jobs: list[Job]) -> list[Reference]:
        references = []
        for job in jobs:
            cursor.execute(
                self._push_job_sql(),
                (
                    chancy_uuid(),
                    job.queue,
                    json_dumps(
                        {
                            "func": job.func,
                            "kwargs": job.kwargs or {},
                            "limits": [
                                limit.serialize() for limit in job.limits
                            ],
                        }
                    ),
                    job.priority,
                    job.max_attempts,
                    job.scheduled_at,
                    job.unique_key,
                ),
            )
            record = cursor.fetchone()
            references.append(Reference(record[0]))

        if self.notifications:
            for queue in set(job.queue for job in jobs):
                self.notify(cursor, "queue.pushed", {"q": queue})

        return references

    def get_job(self, ref: Reference) -> JobInstance:
        with self.pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cursor:
                cursor.execute(self._get_job_sql(), [ref.identifier])
                record = cursor.fetchone()
                if record is None:
                    raise KeyError(f"Job {ref.identifier} not found.")
                return JobInstance.unpack(record)

    def wait_for_job(
        self, ref: Reference, *, interval: int = 1
    ) -> JobInstance | None:
        while True:
            job = self.get_job(ref)
            if job is None or job.state in {
                JobInstance.State.SUCCEEDED,
                JobInstance.State.FAILED,
            }:
                return job
            time.sleep(interval)

    def get_all_queues(self) -> list[Queue]:
        with self.pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cursor:
                cursor.execute(self._get_all_queues_sql())
                return [Queue.unpack(record) for record in cursor]

    def get_queue(self, name: str) -> Queue:
        with self.pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cursor:
                cursor.execute(self._get_queue_sql(), [name])
                record = cursor.fetchone()
                if record is None:
                    raise KeyError(f"Queue {name!r} not found.")
                return Queue.unpack(record)

    def __iter__(self):
        return iter(self.get_all_queues())

    def notify(self, cursor, event: str, payload: dict[str, Any]):
        cursor.execute(
            "SELECT pg_notify(%s, %s)",
            [
                f"{self.prefix}events",
                json_dumps({"t": event, **payload}),
            ],
        )


class Chancy(BaseChancy):
    """
    An asyncio-compatible version of the Chancy application.

    When possible, this is the preferred version of the Chancy application to
    use, as it allows for more efficient use of system resources and better
    integration with other asyncio-based libraries.

    .. code-block:: python

        import asyncio
        from chancy import Chancy, Job, Queue

        def my_job():
            print("Hello, world!")

        async def main():
            async with Chancy("postgresql://localhost/chancy") as chancy:
                await chancy.migrate()
                await chancy.declare(Queue("default"))
                await chancy.push(Job.from_func(my_job))

        asyncio.run(main())
    """

    @cached_property
    def pool(self):
        return AsyncConnectionPool(
            self.dsn,
            min_size=self.min_connection_pool_size,
            max_size=self.max_connection_pool_size,
            reconnect_timeout=self.poll_reconnect_timeout,
            # Opening the pool here is deprecated, and should be done
            # explicitly by the user with a context manager or open/close.
            open=False,
        )

    async def __aenter__(self):
        await self.pool.open()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.pool.close()
        return False

    async def migrate(self, *, to_version: int | None = None):
        migrator = Migrator("chancy", "chancy.migrations", prefix=self.prefix)
        async with self.pool.connection() as conn:
            await migrator.migrate(conn, to_version=to_version)

        for plugin in self.plugins:
            await plugin.migrate(self, to_version=to_version)

    async def declare(self, queue: Queue, *, upsert: bool = False) -> Queue:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cursor:
                return await self.declare_ex(cursor, queue, upsert=upsert)

    async def declare_ex(
        self, cursor: AsyncCursor, queue: Queue, *, upsert: bool = False
    ) -> Queue:
        await cursor.execute(
            self._declare_sql(upsert),
            {
                "name": queue.name,
                "state": queue.state.value,
                "concurrency": queue.concurrency,
                "tags": list(queue.tags),
                "executor": queue.executor,
                "executor_options": Json(queue.executor_options),
                "polling_interval": queue.polling_interval,
                "rate_limit": queue.rate_limit,
                "rate_limit_window": queue.rate_limit_window,
            },
        )

        result = await cursor.fetchone()
        return Queue(
            name=queue.name,
            state=result[0],
            concurrency=result[1],
            tags=set(result[2]),
            polling_interval=result[3],
            executor=result[4],
            executor_options=result[5],
            rate_limit=result[6],
            rate_limit_window=result[7],
        )

    async def push(self, job: Job) -> Reference:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cursor:
                return (await self.push_many_ex(cursor, [job]))[0]

    async def push_many(
        self, jobs: list[Job], *, batch_size: int = 1000
    ) -> Iterator[list[Reference]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cursor:
                for chunk in chunked(jobs, batch_size):
                    async with conn.transaction():
                        yield await self.push_many_ex(cursor, chunk)

    async def push_many_ex(
        self, cursor: AsyncCursor, jobs: list[Job]
    ) -> list[Reference]:
        references = []
        for job in jobs:
            await cursor.execute(
                self._push_job_sql(),
                (
                    chancy_uuid(),
                    job.queue,
                    json_dumps(
                        {
                            "func": job.func,
                            "kwargs": job.kwargs or {},
                            "limits": [
                                limit.serialize() for limit in job.limits
                            ],
                        }
                    ),
                    job.priority,
                    job.max_attempts,
                    job.scheduled_at,
                    job.unique_key,
                ),
            )
            record = await cursor.fetchone()
            references.append(Reference(record[0]))

        if self.notifications:
            for queue in set(job.queue for job in jobs):
                await self.notify(cursor, "queue.pushed", {"q": queue})

        return references

    async def get_job(self, ref: Reference) -> JobInstance:
        async with self.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(self._get_job_sql(), [ref.identifier])
                record = await cursor.fetchone()
                if record is None:
                    raise KeyError(f"Job {ref.identifier} not found.")
                return JobInstance.unpack(record)

    async def wait_for_job(
        self, ref: Reference, *, interval: int = 1
    ) -> JobInstance | None:
        while True:
            job = await self.get_job(ref)
            if job is None or job.state in {
                JobInstance.State.SUCCEEDED,
                JobInstance.State.FAILED,
            }:
                return job
            await asyncio.sleep(interval)

    async def get_all_queues(self) -> list[Queue]:
        async with self.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(self._get_all_queues_sql())
                return [Queue.unpack(record) async for record in cursor]

    async def get_queue(self, name: str) -> Queue:
        async with self.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(self._get_queue_sql(), [name])
                record = await cursor.fetchone()
                if record is None:
                    raise KeyError(f"Queue {name!r} not found.")
                return Queue.unpack(record)

    async def __aiter__(self):
        async with self.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(self._get_all_queues_sql())
                async for record in cursor:
                    yield Queue.unpack(record)

    async def notify(self, cursor, event: str, payload: dict[str, Any]):
        await cursor.execute(
            "SELECT pg_notify(%s, %s)",
            [
                f"{self.prefix}events",
                json_dumps({"t": event, **payload}),
            ],
        )
