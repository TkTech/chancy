import asyncio
import json
import uuid
from typing import Any
from asyncio import TaskGroup

from psycopg import sql
from psycopg import AsyncConnection, AsyncCursor

from chancy.app import Chancy
from chancy.hub import Hub
from chancy.plugin import PluginScope
from chancy.logger import PrefixAdapter, logger


class Worker:
    """
    The Worker is responsible for polling queues for new jobs, running any
    configured plugins, and internal management such as heartbeats.

    As an example, lets create a Worker that will poll the "default" queue
    with a concurrency of 10, and run a single worker:

    .. code-block:: python

        async with Chancy(
            dsn="postgresql://localhost/postgres",
            queues=[
                Queue(name="default", concurrency=10),
            ],
        ) as chancy:
            await chancy.migrate()
            await Worker(chancy).start()

    .. note::

        Don't share plugin instances between workers. Each worker should have
        its own instance of each plugin.

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
        #: An event hub for tracing and debugging.
        self.hub = Hub()
        #: An event that is set when the worker is the leader.
        #: This functionality is not enabled by default - a leadership plugin
        #: must be used to enable this event.
        self.is_leader = asyncio.Event()

    async def start(self):
        """
        Start the worker.

        Will run indefinitely, polling the queues for new jobs and running any
        configured plugins.
        """
        async with self.chancy.pool.connection() as conn:
            self.logger.debug(
                f"Performing initial worker announcement using worker ID"
                f" {self.worker_id!r}."
            )
            await self.announce_worker(conn)

        async with TaskGroup() as group:
            group.create_task(self.maintain_heartbeat())
            group.create_task(self.maintain_notifications())

            for plugin in self.chancy.plugins:
                match plugin.get_scope():
                    case PluginScope.WORKER:
                        group.create_task(plugin.run(self, self.chancy))
                    case PluginScope.QUEUE:
                        group.create_task(plugin.run(self, self.chancy))

    async def maintain_heartbeat(self):
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

    async def maintain_notifications(self):
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
            match j["t"]:
                case "pushed":
                    try:
                        await self[j["q"]].wake_up()
                    except KeyError:
                        # This worker isn't setup to process this queue, so
                        # we'll just ignore it.
                        continue
                case _:
                    await self.hub.emit(j["t"], j)

    async def announce_worker(self, conn: AsyncConnection):
        """
        Announce the worker to the cluster.

        This will insert the worker into the workers table, or update the
        `last_seen` timestamp if the worker is already present.

        :param conn: The connection to use for the announcement.
        """
        async with conn.cursor() as cur:
            await cur.execute(
                sql.SQL(
                    """
                    INSERT INTO {workers}
                        (worker_id, last_seen)
                    VALUES (%s, NOW())
                    ON CONFLICT (worker_id) DO UPDATE
                        SET last_seen = NOW()
                    """
                ).format(
                    workers=sql.Identifier(f"{self.chancy.prefix}workers")
                ),
                [self.worker_id],
            )
            await self.notify(
                cur, "worker.started", {"worker_id": self.worker_id}
            )

        await self.hub.emit("worker.started")

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
            await self.notify(
                cur, "worker.stopped", {"worker_id": self.worker_id}
            )

        await self.hub.emit("worker.stopped")

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
        await cursor.execute(
            sql.SQL(
                """
                SELECT pg_notify(%s, %s)
                """
            ),
            [
                f"{self.chancy.prefix}events",
                json.dumps({"t": event, **payload}),
            ],
        )

    def __repr__(self):
        return f"<Worker({self.worker_id!r})>"
