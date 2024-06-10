import asyncio
import uuid
from asyncio import TaskGroup
from functools import cached_property

from psycopg_pool import AsyncConnectionPool

from chancy.migrate import Migrator
from chancy.queue import Queue


class Chancy:
    """
    A Chancy application.

    :param dsn: The DSN for the PostgreSQL database.
    :param queues: A list of queues to poll for jobs.
    :param prefix: The prefix to use for all Chancy database tables.
    :param min_connection_pool_size: The minimum number of connections to keep
                                        in the pool.
    :param max_connection_pool_size: The maximum number of connections to keep
                                        in the pool.
    """

    def __init__(
        self,
        dsn: str,
        *,
        queues: list[Queue] | None = None,
        prefix: str = "chancy_",
        min_connection_pool_size: int = 1,
        max_connection_pool_size: int = 10,
    ):
        self.dsn = dsn
        self.queues = queues or []
        self.prefix = prefix
        self.min_connection_pool_size = min_connection_pool_size
        self.max_connection_pool_size = max_connection_pool_size

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
        Get the connection pool.
        """
        return AsyncConnectionPool(
            self.dsn,
            open=False,
            check=AsyncConnectionPool.check_connection,
            min_size=self.min_connection_pool_size,
            max_size=self.max_connection_pool_size,
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

    async def start_worker(self, *, worker_id: str | None = None):
        """
        Start the worker.

        This task will run indefinitely, polling the queues for new jobs,
        running any configured plugins, and managing leadership election.

        :param worker_id: The ID of the worker, which must be globally unique.
                          If not provided, a UUID4 will be generated.
        """
        async with TaskGroup() as group:
            for queue in self.queues:
                # Each queue gets its own task, which will periodically
                # poll the queue for new jobs.
                group.create_task(
                    queue.poll(self, worker_id=worker_id or str(uuid.uuid4()))
                )

    async def push(self, queue_name: str, *jobs):
        """
        Push one or more jobs onto a queue.

        :param queue_name: The name of the queue to push the job onto.
        :param jobs: The jobs to push onto the queue.
        """
        async with self.pool.connection() as conn:
            try:
                queue = next(q for q in self.queues if q.name == queue_name)
            except StopIteration:
                raise ValueError(f"Queue {queue_name!r} not found")

            await queue.push_jobs(conn, list(jobs), prefix=self.prefix)

    def push_sync(self, queue_name: str, *jobs):
        """
        Push one or more jobs onto a queue.

        This is a shim to allow synchronous code to push jobs onto a queue.
        If an event loop is already running, the jobs will be pushed onto the
        queue asynchronously. If no event loop is running, a new one will be
        created and run until the jobs have been pushed.

        It is recommended to use the asynchronous `push` method instead of this
        method whenever possible.

        :param queue_name: The name of the queue to push the job onto.
        :param jobs: The jobs to push onto the queue.
        """
        queue = next(q for q in self.queues if q.name == queue_name)

        async def push_jobs():
            async with self.pool.connection() as conn:
                await queue.push_jobs(conn, list(jobs), prefix=self.prefix)

        existing_loop = asyncio.get_event_loop()
        if existing_loop.is_running():
            asyncio.run_coroutine_threadsafe(push_jobs(), existing_loop)
        else:
            asyncio.get_event_loop().run_until_complete(push_jobs())
