import dataclasses
from functools import cached_property

from psycopg_pool import AsyncConnectionPool

from chancy.migrate import Migrator
from chancy.queue import Queue


@dataclasses.dataclass(frozen=True, kw_only=True)
class Chancy:
    """
    The main application object for Chancy.

    This object is responsible for managing the connection pool to the database,
    general configuration, and provides some helpers for common tasks like
    pushing jobs.
    """

    #: The DSN to use to connect to the database.
    dsn: str
    #: The queues that are managed by the Chancy application.
    queues: list[Queue] = dataclasses.field(default_factory=list)
    #: The prefix to use for all Chancy database tables.
    prefix: str = "chancy_"
    #: The minimum number of connections to keep in the connection pool.
    min_connection_pool_size: int = 1
    #: The maximum number of connections to keep in the connection pool.
    max_connection_pool_size: int = 10
    #: The number of seconds to wait before attempting to reconnect to the
    #: database after a connection is lost from the pool.
    poll_reconnect_timeout: int = 60 * 5

    def __post_init__(self):
        self._sanity_check()

    async def migrate(self, *, to_version: int | None = None):
        """
        Migrate the database to the latest schema version.

        If `to_version` is provided, the database will be migrated to that
        specific version, up or down as necessary.
        """
        migrator = Migrator("chancy", "chancy.migrations", prefix=self.prefix)
        async with self.pool.connection() as conn:
            await migrator.migrate(conn, to_version=to_version)

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

    async def push(self, queue: str | Queue, *jobs):
        """
        Push one or more jobs onto a queue.

        All jobs will be committed as part of the same INSERT and transaction.

        :param queue: The name or Queue of the queue to push the job onto.
        :param jobs: The jobs to push onto the queue.
        """
        async with self.pool.connection() as conn:
            if isinstance(queue, Queue):
                await queue.push_jobs(conn, list(jobs), prefix=self.prefix)
                return

            try:
                queue = next(q for q in self.queues if q.name == queue)
            except StopIteration:
                raise ValueError(f"Queue {queue!r} not found")

            await queue.push_jobs(conn, list(jobs), prefix=self.prefix)

    def _sanity_check(self):
        """
        Perform a sanity check on the Chancy application.
        """
        if len(set(q.name.lower() for q in self.queues)) != len(self.queues):
            raise ValueError("Queue names must be unique")
