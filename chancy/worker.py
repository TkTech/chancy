import asyncio
import json
import uuid
from typing import Any
from asyncio import TaskGroup
from datetime import datetime

from psycopg import sql
from psycopg import AsyncConnection, AsyncCursor

from chancy.app import Chancy
from chancy.hub import Hub
from chancy.plugins.plugin import Plugin
from chancy.logger import PrefixAdapter, logger


class Worker:
    """
    The Worker is responsible for polling queues for new jobs, running any
    configured plugins, and internal management such as heartbeats and
    cluster leadership.

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
    :param plugins: A list of plugins to run with the worker.
    :param leadership_poll_interval: The number of seconds between leadership
                                     poll intervals.
    :param leadership_timeout: The number of seconds before a worker is
                               considered to have lost leadership.
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
        plugins: list["Plugin"] = None,
        leadership_poll_interval: int = 60,
        leadership_timeout: int = 60 * 3,
        heartbeat_poll_interval: int = 30,
        heartbeat_timeout: int = 90,
    ):
        #: The Chancy application that the worker is associated with.
        self.chancy = chancy
        #: The ID of the worker, which must be globally unique.
        self.worker_id = worker_id or str(uuid.uuid4())
        #: The logger used by an active worker.
        self.logger = PrefixAdapter(logger, {"prefix": "Worker"})
        #: The table used to store leadership information.
        self.leadership_table = sql.Identifier(f"{self.chancy.prefix}leader")
        #: The number of seconds between leadership poll intervals.
        self.leadership_poll_interval = leadership_poll_interval
        #: The number of seconds before a worker is considered to have lost
        #: leadership.
        self.leadership_timeout = leadership_timeout
        #: The number of seconds between heartbeat poll intervals.
        self.heartbeat_poll_interval = heartbeat_poll_interval
        #: The number of seconds before a worker is considered to have lost
        #: connection to the cluster.
        self.heartbeat_timeout = heartbeat_timeout
        #: The plugins that are associated with the worker.
        self.plugins = plugins or []
        #: An event hub for tracing and debugging.
        self.hub = Hub()

        self.is_leader = asyncio.Event()

    async def start(self):
        """
        Start the worker.

        Will run indefinitely, polling the queues for new jobs, running any
        configured plugins, and managing leadership election.
        """
        async with self.chancy.pool.connection() as conn:
            self.logger.debug(
                f"Performing initial worker announcement using worker ID"
                f" {self.worker_id!r}."
            )
            await self.announce_worker(conn)

        async with TaskGroup() as group:
            group.create_task(self.maintain_leadership())
            group.create_task(self.maintain_heartbeat())
            group.create_task(self.maintain_notifications())

            for plugin in self.plugins:
                group.create_task(plugin.run(self, self.chancy))

            for queue in self.chancy.queues:
                group.create_task(
                    queue.poll(
                        self.chancy,
                        self,
                    )
                )

    async def maintain_leadership(self):
        """
        Attempt to gain and maintain leadership of the cluster.

        .. note::

            This method should not be called directly. It is automatically
            run when the worker is started.
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

        await self.hub.emit("worker.announced")

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

        await self.hub.emit("worker.revoked")

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
                SELECT pg_notify({channel}, %s)
                """
            ).format(channel=sql.Identifier(f"{self.chancy.prefix}events")),
            [json.dumps({"t": event, **payload})],
        )

    async def _check_and_update_leadership(self, conn: AsyncConnection):
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

    async def _gain_leadership(self, cur: AsyncCursor, leader_id: str):
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

    async def _gain_leadership_first_time(self, cur: AsyncCursor):
        """
        Insert a new entry in the leadership table to gain leadership for the
        first time.
        """
        await cur.execute(
            sql.SQL(
                """
                INSERT INTO {table}
                    (worker_id, last_seen)
                VALUES (%s, NOW())
                """
            ).format(table=self.leadership_table),
            [self.worker_id],
        )
        await self.on_gained_leadership()

    async def _maintain_leadership(self, cur: AsyncCursor, leader_id: str):
        """
        Update the leadership table to maintain leadership.
        """
        await cur.execute(
            sql.SQL(
                """
                UPDATE {table}
                SET last_seen = NOW()
                WHERE id = %s AND worker_id = %s
                """
            ).format(table=self.leadership_table),
            [leader_id, self.worker_id],
        )
        await self.on_maintained_leadership()

    async def on_gained_leadership(self):
        """
        Called when the worker gains leadership of the cluster.
        """
        self.logger.info("Gained cluster leadership.")
        self.is_leader.set()
        await self.hub.emit("leadership.gained")

    async def on_maintained_leadership(self):
        """
        Called when the worker maintains an existing leadership of the cluster.
        """
        self.logger.debug("Maintained existing cluster leadership.")
        self.is_leader.set()
        await self.hub.emit("leadership.maintained")

    async def on_lost_leadership(self):
        """
        Called when the worker either loses leadership of the cluster, or
        if it was unable to acquire leadership.
        """
        if self.is_leader.is_set():
            self.logger.info("Lost cluster leadership.")
            self.is_leader.clear()
            await self.hub.emit("leadership.lost")
        else:
            self.logger.debug("Not the current leader of the cluster.")
            self.is_leader.clear()

    def __getitem__(self, key: str):
        return self.chancy.queues_by_name[key]

    def __contains__(self, key: str):
        return key in self.chancy.queues_by_name

    def __iter__(self):
        return iter(self.chancy.queues)

    def __len__(self):
        return len(self.chancy.queues)

    def __repr__(self):
        return f"<Worker({self.worker_id!r})>"
