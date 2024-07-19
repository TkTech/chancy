import re
import asyncio
import json
import uuid
import socket
import platform

from psycopg import sql
from psycopg import AsyncConnection
from psycopg.rows import dict_row

from chancy.app import Chancy
from chancy.hub import Hub
from chancy.queue import Queue
from chancy.logger import PrefixAdapter, logger


class TaskManager:
    """
    A simple task manager that keeps track of tasks.

    The asyncio.TaskGroup is not used here for a couple of reasons, but mostly
    because it's only available in Python 3.11+ and we want to maintain
    compatibility with earlier versions.
    """

    def __init__(self):
        self.tasks: set[asyncio.Task] = set()

    async def add(self, coro, *, name: str | None = None):
        """
        Add a task to the manager.
        """
        task = asyncio.create_task(coro)
        self.tasks.add(task)
        if name:
            task.set_name(name)
        return task

    async def remove(self, task: asyncio.Task):
        """
        Remove a task from the manager.
        """
        task.cancel()
        self.tasks.remove(task)

    async def run(self):
        """
        Run all tasks until they are complete.
        """
        while self.tasks:
            done, pending = await asyncio.wait(
                self.tasks, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                self.tasks.remove(task)
                try:
                    task.result()
                except asyncio.CancelledError:
                    logger.debug(f"Task {task.get_name()} was cancelled.")

    async def shutdown(self):
        """
        Cancel all tasks and wait for them to complete.
        """
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()


class Worker:
    """
    The Worker is responsible for polling queues for new jobs, running any
    configured plugins, and internal management such as heartbeats.

    As an example, lets create a Worker that will poll the "default" queue
    with a concurrency of 10, and run a single worker:

    .. code-block:: python

        async with Chancy(
            dsn="postgresql://localhost/postgres",
        ) as chancy:
            await chancy.migrate()
            await Worker(chancy).start()

    :param chancy: The Chancy application that the worker is associated with.
    :param worker_id: The ID of the worker, which must be globally unique. If
                      not provided, a random UUID will be generated.
    :param heartbeat_poll_interval: The number of seconds between heartbeat
                                    poll intervals.
    :param heartbeat_timeout: The number of seconds before a worker is
                              considered to have lost connection to the
                              cluster.
    """

    def __init__(
        self,
        chancy: Chancy,
        *,
        worker_id: str | None = None,
        heartbeat_poll_interval: int = 30,
        heartbeat_timeout: int = 90,
        queue_change_poll_interval: int = 10,
        tags: set[str] | None = None,
    ):
        #: The Chancy application that the worker is associated with.
        self.chancy = chancy
        #: The ID of the worker, which must be globally unique.
        self.worker_id = worker_id or str(uuid.uuid4())
        #: The logger used by an active worker.
        self.logger = PrefixAdapter(logger, {"prefix": "Worker"})
        #: The number of seconds between heartbeat poll intervals.
        self.heartbeat_poll_interval = heartbeat_poll_interval
        #: The number of seconds before a worker is considered to have lost
        #: connection to the cluster.
        self.heartbeat_timeout = heartbeat_timeout
        #: How frequently to check for changes to the queues that this worker
        #: should be processing.
        self.queue_change_poll_interval = queue_change_poll_interval
        #: An event hub with cluster and local telemetry, useful for plugins.
        self.hub = Hub()
        #: An event that is set when the worker is the leader.
        #: This functionality is not enabled by default - a leadership plugin
        #: must be used to enable this event.
        self.is_leader = asyncio.Event()
        # Extra tags to associate with the worker.
        self._extra_tags = tags or set()

        self._queues: dict[str, Queue] = {}
        self.manager = TaskManager()

    async def start(self):
        """
        Start the worker.

        Will run indefinitely, polling the queues for new jobs and running any
        configured plugins.
        """
        # As a QoL, we sync the queues first thing to ensure they're up-to-
        # date before the heartbeat runs.
        await self._sync_queues()
        await self.manager.add(self._maintain_queues(), name="queues")
        await self.manager.add(self._maintain_heartbeat(), name="heartbeat")

        # pgbouncer and other connection pools may not support LISTEN/NOTIFY
        # properly, so this needs to remain an optional (but default) feature.
        if self.chancy.notifications:
            await self.manager.add(
                self._maintain_notifications(), name="notifications"
            )

        for plugin in self.chancy.plugins:
            await self.manager.add(
                plugin.run(self, self.chancy),
                name=plugin.__class__.__name__,
            )
            self.logger.info(f"Started plugin {plugin.__class__.__name__!r}")

        try:
            await self.manager.run()
        except asyncio.CancelledError:
            await self.manager.shutdown()

    async def _maintain_queues(self):
        """
        Maintain the queues that the worker is processing.

        This will periodically check the database for any changes to the queues
        that the worker should be processing, and update the worker's queues
        accordingly.

        .. note::

            This method should not be called directly. It is automatically
            run when the worker is started.
        """
        while True:
            await self._sync_queues()
            await asyncio.sleep(self.queue_change_poll_interval)

    async def _maintain_heartbeat(self):
        """
        Announces the worker to the cluster, and maintains a periodic heartbeat
        to ensure that the worker is still alive.

        .. note::

            This method should not be called directly. It is automatically
            run when the worker is started.
        """
        while True:
            async with self.chancy.pool.connection() as conn:
                async with conn.transaction():
                    self.logger.debug("Announcing worker to the cluster.")
                    await self.announce_worker(conn)
            await asyncio.sleep(self.heartbeat_poll_interval)

    async def _maintain_notifications(self):
        """
        Listen for notifications from the database.

        Improves the reactivity of a worker by allowing it to almost immediately
        react to database events using Postgres's LISTEN/NOTIFY feature.

        .. note::

            This feature utilizes a permanent connection to the database
            separate from the shared connection pool.

        .. note::

            This method should not be called directly. It is automatically
            run when the worker is started.
        """
        connection = await AsyncConnection.connect(
            self.chancy.dsn, autocommit=True
        )
        await connection.execute(
            sql.SQL("LISTEN {channel};").format(
                channel=sql.Identifier(f"{self.chancy.prefix}events")
            )
        )
        self.logger.info("Started listening for realtime notifications.")
        async for notification in connection.notifies():
            j = json.loads(notification.payload)
            event = j.pop("t")
            if event == "pushed":
                try:
                    self._queues[j["q"]].wake_up()
                except KeyError:
                    # This worker isn't setup to process this queue, so
                    # we'll just ignore it.
                    pass

            await self.hub.emit(event, j)

    async def announce_worker(self, conn: AsyncConnection):
        """
        Announce the worker to the cluster.

        This will insert the worker into the workers table, or update the
        `last_seen` timestamp if the worker is already present.

        :param conn: The connection to use for the announcement.
        """
        async with conn.cursor() as cur:
            async with conn.transaction():
                await cur.execute(
                    sql.SQL(
                        """
                        DELETE FROM {workers}
                        WHERE expires_at < NOW()
                        """
                    ).format(
                        workers=sql.Identifier(f"{self.chancy.prefix}workers")
                    )
                )
                await cur.execute(
                    sql.SQL(
                        """
                        INSERT INTO {workers}
                            (worker_id, last_seen, expires_at, tags, queues)
                        VALUES (
                            %(worker_id)s,
                            NOW(),
                            NOW() + INTERVAL {timeout},
                            %(tags)s,
                            %(queues)s
                        )
                        ON CONFLICT (worker_id) DO UPDATE
                            SET last_seen = NOW(),
                                expires_at = NOW() + INTERVAL {timeout},
                                tags = EXCLUDED.tags,
                                queues = EXCLUDED.queues
                        """
                    ).format(
                        workers=sql.Identifier(f"{self.chancy.prefix}workers"),
                        timeout=sql.Literal(
                            f"{self.heartbeat_timeout} seconds"
                        ),
                    ),
                    {
                        "worker_id": self.worker_id,
                        "tags": list(self.worker_tags()),
                        "queues": list(self._queues.keys()),
                    },
                )
                await self.chancy.notify(
                    cur, "worker.announced", {"worker_id": self.worker_id}
                )

    async def revoke_worker(self, conn: AsyncConnection):
        """
        Revoke the worker from the cluster.

        This will remove the worker from the workers table, making it appear
        as though the worker has gone offline.

        :param conn: The connection to use for the revocation.
        """
        async with conn.cursor() as cur:
            await cur.execute(
                sql.SQL(
                    """
                    DELETE FROM {workers}
                    WHERE worker_id = %s
                    """
                ).format(
                    workers=sql.Identifier(f"{self.chancy.prefix}workers")
                ),
                [self.worker_id],
            )
            await self.chancy.notify(
                cur, "worker.stopped", {"worker_id": self.worker_id}
            )

    def __repr__(self):
        return f"<Worker({self.worker_id!r})>"

    def worker_tags(self) -> set[str]:
        """
        Return the tags associated with the worker.

        Tags are used to limit which Queues get assigned to a worker, and
        may also be used by some plugins.

        :return: A set of tags associated with the worker.
        """
        return {
            "*",
            f"hostname={socket.gethostname()}",
            f"worker_id={self.worker_id}",
            f"python={platform.python_version()}",
            f"os={platform.system()}",
            f"arch={platform.machine()}",
            *self._extra_tags,
        }

    async def queue_changes(
        self, conn: AsyncConnection
    ) -> tuple[list[Queue], list[Queue]]:
        """
        Check the database for any changes to the queues that this worker
        should be processing, returning a list of queues that were added and
        a list of queues that were removed.

        :param conn: The connection to use for the query.
        :return: A tuple containing a list of queues that were added and a
                 list of queues that were removed.
        """
        added = []
        removed = []

        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                sql.SQL("SELECT * FROM {queues}").format(
                    queues=sql.Identifier(f"{self.chancy.prefix}queues")
                ),
                {
                    "tags": list(self.worker_tags()),
                },
            )
            results = {}
            worker_tags = self.worker_tags()
            for queue in await cursor.fetchall():
                if any(
                    any(
                        tag == worker_tag or re.match(tag, worker_tag)
                        for worker_tag in worker_tags
                    )
                    for tag in queue["tags"]
                ):
                    results[queue["name"]] = queue

            # Find all queues which are not currently assigned to
            # this worker.
            existing = set(self._queues.keys())
            new = set(results.keys())

            # Remove any queues that are no longer assigned to this
            # worker.
            for queue in existing - new:
                q = self._queues.get(queue)
                if q:
                    removed.append(q)

            # Add any new queues that are assigned to this worker.
            for queue in new - existing:
                q = results[queue]
                added.append(
                    Queue(
                        name=q["name"],
                        concurrency=q["concurrency"],
                        tags=q["tags"],
                        state=q["state"],
                        executor=q["executor"],
                        executor_options=q["executor_options"],
                        polling_interval=q["polling_interval"],
                    )
                )

        return added, removed

    async def _sync_queues(self):
        async with self.chancy.pool.connection() as conn:
            added, removed = await self.queue_changes(conn)
            for queue in added:
                self.logger.info(f"Adding queue {queue.name!r}.")
                self._queues[queue.name] = queue
                await self.manager.add(
                    queue.run(self, self.chancy), name=f"queue.{queue.name}"
                )

            for queue in removed:
                self.logger.info(f"Removing queue {queue.name!r}.")
                del self._queues[queue.name]

            async with conn.cursor() as cursor:
                for queue in self._queues.values():
                    await queue.declare(self.chancy, cursor)
