import uuid
from asyncio import TaskGroup
from functools import cached_property

from psycopg_pool import AsyncConnectionPool

from chancy.migrate import Migrator
from chancy.queue import Queue


class Worker:
    """
    A standard queue worker.

    """

    def __init__(
        self,
        dsn: str,
        *,
        queues: list[Queue] | None = None,
        prefix: str = "chancy_",
        min_connection_pool_size: int = 1,
        max_connection_pool_size: int = 10,
        worker_id: str | None = None,
    ):
        self.dsn = dsn
        self.queues = queues or []
        self.prefix = prefix
        self.min_connection_pool_size = min_connection_pool_size
        self.max_connection_pool_size = max_connection_pool_size
        self.worker_id = worker_id or str(uuid.uuid4())

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

    async def start(self):
        """
        Start the worker.

        This will start the worker and begin processing jobs from the queues.
        """
        async with TaskGroup() as group:
            for queue in self.queues:
                # Each queue gets its own task, which will periodically
                # poll the queue for new jobs.
                group.create_task(queue.poll(self))
