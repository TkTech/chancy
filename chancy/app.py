import json
import dataclasses
from functools import cached_property
from typing import Any

from psycopg import sql
from psycopg import AsyncCursor
from psycopg_pool import AsyncConnectionPool

from chancy.migrate import Migrator
from chancy.job import Job
from chancy.queue import Queue
from chancy.job import Reference
from chancy.plugin import Plugin


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

    async def push(self, queue: str, job: Job) -> Reference:
        """
        Push a single job onto the queue.

        .. note::

            A Job with a `unique_key` will not be pushed if a job with the same
            `unique_key` is already in the queue. The existing job will be
            returned instead.

        :param queue: The name of the queue to push the job onto.
        :param job: The job to push onto the queue.
        :return: The reference to the job that was pushed.
        """
        return (await Queue(queue).push(self, [job]))[0]

    async def push_many(self, queue: str, *jobs: Job) -> list[Reference]:
        """
        Push one or more jobs onto a queue.

        .. note::

            A Job with a `unique_key` will not be pushed if a job with the same
            `unique_key` is already in the queue. The existing job will be
            returned instead.

        :param queue: The name of the queue to push the jobs onto.
        :param jobs: The jobs to push onto the queue.
        """
        return await Queue(queue).push(self, list(jobs))

    async def notify(
        self, cursor: AsyncCursor, event: str, payload: dict[str, Any]
    ):
        """
        Notify the cluster of an event.

        .. note::

            This method does not start or end a transaction. It is up to the
            caller to manage the transaction.

        :param cursor: The cursor to use for the notification.
        :param event: The event to notify the cluster of.
        :param payload: The payload to send with the notification.
        """
        if not self.notifications:
            return

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

    async def declare(self, queue: Queue, *, upsert: bool = False):
        """
        Declare a queue for the cluster to process.
        """
        async with self.pool.connection() as conn:
            async with conn.cursor() as cursor:
                await queue.declare(self, cursor, upsert=upsert)
