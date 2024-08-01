import json
import logging
import dataclasses
from typing import Any, Iterator
from functools import cached_property, cache

from psycopg import sql
from psycopg import AsyncCursor
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from psycopg.types.json import Json

from chancy.migrate import Migrator
from chancy.queue import Queue
from chancy.job import Reference, Job, JobInstance
from chancy.plugin import Plugin
from chancy.utils import chancy_uuid, chunked


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


@dataclasses.dataclass(kw_only=True)
class Chancy:
    """
    The main application object for Chancy.

    This object is responsible for managing the connection pool to the database,
    general configuration, and provides some helpers for common tasks like
    pushing jobs.
    """

    #: The DSN to use to connect to the database.
    dsn: str
    #: The list of plugins that are to be used by the Chancy application.
    plugins: list[Plugin] = dataclasses.field(default_factory=list)
    #: The prefix to use for all Chancy database tables.
    prefix: str = "chancy_"
    #: The minimum number of connections to keep in the connection pool.
    min_connection_pool_size: int = 1
    #: The maximum number of connections to keep in the connection pool.
    max_connection_pool_size: int = 10
    #: The number of seconds to wait before attempting to reconnect to the
    #: database after a connection is lost from the pool.
    poll_reconnect_timeout: int = 60 * 5
    #: If set, the Chancy application will emit notifications for various
    #: events.
    notifications: bool = True
    #: The logger to use for the application.
    log: logging.Logger = dataclasses.field(
        default_factory=_setup_default_logger
    )

    async def migrate(self, *, to_version: int | None = None):
        """
        Migrate the database to the latest schema version.

        This convenience method will migrate all plugins that have migration
        scripts associated with them as well. If you need to migrate a specific
        plugin, you should call the `migrate` method on the plugin itself.

        If `to_version` is provided, the database will be migrated to that
        specific version, up or down as necessary.
        """
        migrator = Migrator("chancy", "chancy.migrations", prefix=self.prefix)
        async with self.pool.connection() as conn:
            await migrator.migrate(conn, to_version=to_version)

        for plugin in self.plugins:
            await plugin.migrate(self, to_version=to_version)

    @cached_property
    def pool(self):
        """
        An async connection pool to the configured database.
        """
        return AsyncConnectionPool(
            self.dsn,
            open=False,
            check=AsyncConnectionPool.check_connection,
            min_size=self.min_connection_pool_size,
            max_size=self.max_connection_pool_size,
            reconnect_timeout=self.poll_reconnect_timeout,
        )

    async def __aenter__(self):
        await self.pool.open()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.pool.close()
        return False

    async def open(self):
        """
        Open the connection pool.

        Whenever possible, it's preferred to use the async context manager
        instead of this method, as it will ensure that the connection pool is
        properly closed when the block is exited. Ex:

        .. code-block:: python

            async with Chancy(...) as chancy:
                ...
        """
        await self.pool.open()

    async def close(self):
        """
        Close the connection pool.

        Whenever possible, it's preferred to use the async context manager
        instead of this method, as it will ensure that the connection pool is
        properly closed when the block is exited. Ex:

        .. code-block:: python

            async with Chancy(...) as chancy:
                ...
        """
        if not self.pool.closed:
            await self.pool.close()

    async def notify(
        self, cursor: AsyncCursor, event: str, payload: dict[str, Any]
    ):
        """
        Notify the cluster of an event.

        :param cursor: The cursor to use for the notification.
        :param event: The event to notify the cluster of.
        :param payload: The payload to send with the notification.
        """
        await cursor.execute(
            sql.SQL(
                """
                SELECT pg_notify(%s, %s)
                """
            ),
            [
                f"{self.prefix}events",
                json.dumps({"t": event, **payload}),
            ],
        )

    async def declare(self, queue: Queue, *, upsert: bool = False) -> Queue:
        """
        Declare the queue to the cluster, returning a new Queue with any
        updated values.

        :param queue: The queue to declare.
        :param upsert: If set, the queue will be updated if it already exists
                       with the values provided.
        """
        async with self.pool.connection() as conn:
            async with conn.cursor() as cursor:
                await self.declare_ex(cursor, queue, upsert=upsert)
        return queue

    async def declare_ex(
        self, cursor: AsyncCursor, queue: Queue, *, upsert: bool = False
    ) -> Queue:
        """
        Declare the queue to the cluster.

        This advanced method can be used to declare a queue within an existing
        transaction.

        .. note::

            The cursor given to this function should be a dictionary cursor.

        :param cursor: The database cursor to use.
        :param queue: The queue to declare.
        :param upsert: If set, the queue will be updated if it already exists
                          with the values provided.
        """
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
                    polling_interval = EXCLUDED.polling_interval
                """
            )

        await cursor.execute(
            sql.SQL(
                """
                INSERT INTO {queues} (
                    name,
                    state,
                    concurrency,
                    tags,
                    executor,
                    executor_options,
                    polling_interval
                ) VALUES (
                    %(name)s,
                    %(state)s,
                    %(concurrency)s,
                    %(tags)s,
                    %(executor)s,
                    %(executor_options)s,
                    %(polling_interval)s
                )
                ON CONFLICT (name) DO
                    {action}
                RETURNING 
                    state,
                    concurrency,
                    tags,
                    polling_interval,
                    executor,
                    executor_options
                """
            ).format(
                queues=sql.Identifier(f"{self.prefix}queues"),
                action=action,
            ),
            {
                "name": queue.name,
                "state": queue.state,
                "concurrency": queue.concurrency,
                "tags": list(queue.tags),
                "executor": queue.executor,
                "executor_options": Json(queue.executor_options),
                "polling_interval": queue.polling_interval,
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
        )

    async def push(self, job: Job) -> Reference:
        """
        Push a single job onto the queue.

        :param job: The job to push onto the queue.
        """
        async with self.pool.connection() as conn:
            async with conn.cursor() as cursor:
                return (await self.push_many_ex(cursor, [job]))[0]

    async def push_many(
        self, jobs: list[Job], *, batch_size: int = 1000
    ) -> Iterator[Reference]:
        """
        Push one or more jobs onto the queue.

        Yields a reference for each job pushed after its containing transaction
        has been committed.

        :param jobs: The jobs to push onto the queue.
        :param batch_size: The number of jobs to push in a single transaction.
        """
        async with self.pool.connection() as conn:
            async with conn.cursor() as cursor:
                for chunk in chunked(jobs, batch_size):
                    async with conn.transaction():
                        refs = await self.push_many_ex(cursor, chunk)

                    for ref in refs:
                        yield ref

    async def push_many_ex(
        self,
        cursor: AsyncCursor,
        jobs: list[Job],
    ) -> list[Reference]:
        """
        Push one or more jobs onto the queue.

        This advanced method can be used to push jobs to the database
        within an existing transaction. This is very useful when you want to
        push jobs to the queue only if related objects are successfully
        created, like running onboarding for a new user only if the user is
        successfully created.

        :param cursor: The database cursor to use.
        :param jobs: The jobs to push onto the queue.
        """

        # We used to use a single executemany() here, but switched to doing
        # several inserts to better support the RETURNING clause with
        # unique jobs. This allows us to return a reference to the existing
        # job when a conflict occurs, trading performance for convenience.
        references = []
        for job in jobs:
            await cursor.execute(
                # The DO UPDATE clause is used to trick Postgres into returning
                # the existing ID. Otherwise, since no row was touched, the
                # RETURNING clause would get ignored. This isn't ideal - it
                # causes an unnecessary write, but in any normal usage this
                # should be a non-issue.
                sql.SQL(
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
                ).format(
                    jobs=sql.Identifier(f"{self.prefix}jobs"),
                ),
                (
                    chancy_uuid(),
                    job.queue,
                    Json(
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
            references.append(Reference(self, record[0]))

        # Notify the cluster that new jobs have been pushed, allowing workers
        # to wake up and start processing them immediately.
        if self.notifications:
            for queue in set(job.queue for job in jobs):
                await self.notify(
                    cursor,
                    "queue.pushed",
                    {"q": queue},
                )

        return references

    async def get_job(self, ref: Reference) -> JobInstance:
        """
        Retrieve a job from the queue without locking it.

        :param ref: The reference to the job to retrieve.
        """
        async with self.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    sql.SQL(
                        """
                        SELECT
                            *
                        FROM
                            {jobs}
                        WHERE
                            id = %s
                        """
                    ).format(jobs=sql.Identifier(f"{self.prefix}jobs")),
                    [ref.identifier],
                )
                record = await cursor.fetchone()
                if record is None:
                    raise KeyError(f"Job {ref.identifier} not found.")

                return JobInstance.unpack(record)
