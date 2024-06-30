import asyncio
import uuid
from asyncio import TaskGroup
from datetime import datetime
from functools import cached_property

from psycopg import sql
from psycopg_pool import AsyncConnectionPool

from chancy.migrate import Migrator
from chancy.queue import Queue
from chancy.logger import PrefixAdapter, logger
from chancy.plugins.plugin import Plugin, PluginScope


class Chancy:
    """
    The main interface for interacting with a Chancy task queue.

    This class can be used to push jobs to a queue, start a worker process,
    and perform basic database migrations, among other common tasks.

    :param dsn: The DSN for the PostgreSQL database.
    :param queues: A list of queues to use with the application.
    :param prefix: A prefix to apply to all tables.
    :param min_connection_pool_size: The minimum number of connections to keep
                                        in the pool.
    :param max_connection_pool_size: The maximum number of connections to keep
                                        in the pool.
    :param reconnect_timeout: The number of seconds to wait before attempting
                                to reconnect to the database after a connection
                                is lost.
    """

    def __init__(
        self,
        dsn: str,
        *,
        queues: list[Queue] | None = None,
        prefix: str = "chancy_",
        min_connection_pool_size: int = 1,
        max_connection_pool_size: int = 10,
        reconnect_timeout: int = 60 * 5,
        plugins: list["Plugin"] = None,
    ):
        self.dsn = dsn
        self.queues = queues or []
        self.prefix = prefix
        self.min_connection_pool_size = min_connection_pool_size
        self.max_connection_pool_size = max_connection_pool_size
        self.reconnect_timeout = reconnect_timeout
        self.plugins = plugins or []

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
            reconnect_timeout=self.reconnect_timeout,
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
        worker = Worker(
            self,
            worker_id=worker_id,
            plugins=[
                plugin
                for plugin in self.plugins
                if plugin.get_scope() == PluginScope.WORKER
            ],
        )
        await worker.start()

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


class Worker:
    """
    A worker is responsible for polling queues for new jobs, running any
    configured plugins, and managing leadership election.

    :param chancy: The Chancy application that the worker is associated with.
    :param worker_id: The ID of the worker, which must be globally unique.
    """

    def __init__(
        self,
        chancy: Chancy,
        worker_id: str | None = None,
        plugins: list["Plugin"] = None,
    ):
        #: The Chancy application that the worker is associated with.
        self.chancy = chancy
        #: The ID of the worker, which must be globally unique.
        self.worker_id = worker_id or str(uuid.uuid4())
        #: The logger used by an active worker.
        self.logger = PrefixAdapter(logger, {"prefix": f"W.{self.worker_id}"})
        #: The table used to store leadership information.
        self.leadership_table = sql.Identifier(f"{self.chancy.prefix}leader")
        #: The number of seconds between leadership poll intervals.
        self.leadership_poll_interval = 10
        #: The number of seconds before a worker is considered to have lost
        #: leadership.
        self.leadership_timeout = 60
        #: The plugins that are associated with the worker.
        self.plugins = plugins or []

        self._is_leader = False

    async def start(self):
        """
        Start the worker.

        Will run indefinitely, polling the queues for new jobs, running any
        configured plugins, and managing leadership election.
        """
        async with TaskGroup() as group:
            for queue in self.chancy.queues:
                # Each queue gets its own task, which will periodically
                # poll the queue for new jobs.
                group.create_task(
                    queue.poll(self.chancy, worker_id=self.worker_id)
                )
                group.create_task(self.maintain_leadership())

                for plugin in self.plugins:
                    self.logger.info(f"Starting plugin {plugin}")
                    group.create_task(plugin.run(self, self.chancy))

    @property
    def is_leader(self):
        return self._is_leader

    async def maintain_leadership(self):
        """
        Attempt to gain and maintain leadership of the cluster.
        """
        while True:
            try:
                async with self.chancy.pool.connection() as conn:
                    async with conn.transaction():
                        await self._check_and_update_leadership(conn)
            except Exception as e:
                await self.on_lost_leadership()
                raise e

            await asyncio.sleep(self.leadership_poll_interval)

    async def _check_and_update_leadership(self, conn):
        async with conn.cursor() as cur:
            await cur.execute(
                sql.SQL(
                    """
                    SELECT * FROM {prefix} FOR UPDATE
                    """
                ).format(prefix=self.leadership_table)
            )

            result = await cur.fetchone()

            if result:
                leader_id, worker_id, last_seen = result
                now = datetime.now(tz=last_seen.tzinfo)
                if (now - last_seen).total_seconds() > self.leadership_timeout:
                    if worker_id != self.worker_id:
                        await self._gain_leadership(cur, leader_id)
                    else:
                        await self._maintain_leadership(cur, leader_id)
                else:
                    if worker_id == self.worker_id:
                        await self._maintain_leadership(cur, leader_id)
                    else:
                        await self.on_lost_leadership()
            else:
                await self._gain_leadership_first_time(cur)

    async def _gain_leadership(self, cur, leader_id):
        """
        Update the leadership table to gain leadership.
        """
        await cur.execute(
            sql.SQL(
                """
                UPDATE {prefix}
                SET worker_id = %s, last_seen = NOW()
                WHERE id = %s
                """
            ).format(prefix=self.leadership_table),
            [self.worker_id, leader_id],
        )
        await self.on_gained_leadership()

    async def _gain_leadership_first_time(self, cur):
        """
        Insert a new entry in the leadership table to gain leadership for the
        first time.
        """
        await cur.execute(
            sql.SQL(
                """
                INSERT INTO {prefix}leader
                    (worker_id, last_seen)
                VALUES (%s, NOW())
                """
            ).format(prefix=self.leadership_table),
            [self.worker_id],
        )
        await self.on_gained_leadership()

    async def _maintain_leadership(self, cur, leader_id):
        """
        Update the leadership table to maintain leadership.
        """
        await cur.execute(
            sql.SQL(
                """
                UPDATE {prefix}
                SET last_seen = NOW()
                WHERE id = %s AND worker_id = %s
                """
            ).format(prefix=self.leadership_table),
            [leader_id, self.worker_id],
        )
        await self.on_maintained_leadership()

    async def on_gained_leadership(self):
        """
        Called when the worker gains leadership of the cluster.
        """
        self.logger.info("Gained cluster leadership.")
        self._is_leader = True

    async def on_maintained_leadership(self):
        """
        Called when the worker maintains an existing leadership of the cluster.
        """
        self.logger.debug("Maintained existing cluster leadership.")
        self._is_leader = True

    async def on_lost_leadership(self):
        """
        Called when the worker either loses leadership of the cluster, or
        if it was unable to acquire leadership.
        """
        if self._is_leader:
            self.logger.info("Lost cluster leadership.")
        else:
            self.logger.debug("Not the current leader of the cluster.")
        self._is_leader = False
