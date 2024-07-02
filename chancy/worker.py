import asyncio
import uuid
from asyncio import TaskGroup
from datetime import datetime
from functools import cached_property

from psycopg import sql

from chancy.app import Chancy
from chancy.logger import PrefixAdapter, logger
from chancy.plugins.plugin import Plugin


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

    Now, this worker wouldn't be very useful in production, missing some core
    "extras". Chancy implements core functionality as plugins, such as
    abandoned job recovery and completed job pruning. We do this because very
    busy queues may need very specific behavior and queries to perform well,
    and we don't want to force that on everyone. We do provide decent default
    plugins to get you started, though.

    .. code-block:: python

        from chancy.plugins.pruner import Pruner
        from chancy.plugins.recovery import Recovery
        from chancy.plugins.live import Live

        async with Chancy(
            dsn="postgresql://localhost/postgres",
            queues=[
                Queue(name="default", concurrency=10),
            ],
        ) as chancy:
            await chancy.migrate()
            await Worker(
                chancy,
                plugins=[Pruner(), Recovery(), Live()]
            ).start()

    With this, the workers will now run job pruning, abandoned job recovery,
    and live job detection. Writing your own plugins is easy, and you can
    create your own to handle any custom behavior you need. Checkout the
    plugin's documentation for configuration options.

    .. note::

        Don't share plugin instances between workers. Each worker should have
        its own instance of each plugin.

    :param chancy: The Chancy application that the worker is associated with.
    :param worker_id: The ID of the worker, which must be globally unique. If
                      not provided, a random UUID will be generated.
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
        self.leadership_poll_interval = 60
        #: The number of seconds before a worker is considered to have lost
        #: leadership.
        self.leadership_timeout = 60 * 3
        #: The number of seconds between heartbeat poll intervals.
        self.heartbeat_poll_interval = 30
        #: The number of seconds before a worker is considered to have lost
        #: connection to the cluster.
        self.heartbeat_timeout = 90
        #: The plugins that are associated with the worker.
        self.plugins = plugins or []

        self._is_leader = False

    async def start(self):
        """
        Start the worker.

        Will run indefinitely, polling the queues for new jobs, running any
        configured plugins, and managing leadership election.
        """
        async with self.chancy.pool.connection() as conn:
            self.logger.debug("Performing initial worker announcement.")
            await self.announce_worker(conn)

        async with TaskGroup() as group:
            group.create_task(self.maintain_leadership())
            group.create_task(self.maintain_heartbeat())

            for plugin in self.plugins:
                self.logger.info(f"Starting plugin {plugin!r}")
                group.create_task(plugin.run(self, self.chancy))

            for queue in self.chancy.queues:
                group.create_task(
                    queue.poll(
                        self.chancy,
                        worker_id=self.worker_id,
                    )
                )

    @property
    def is_leader(self):
        """
        True if the worker is the current leader of the cluster as of the last
        leadership check.

        .. note::

            The default leadership implementation is "good enough" to ensure
            that only one worker is the leader at any given time, but it's
            possible for a worker to _lose_ leadership _after_ this property
            is checked and before, say, a plugin finishes running. Plugins
            that should be leader-only should keep in mind that more than one
            may be running at once in rare cases.
        """
        return self._is_leader

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

    async def announce_worker(self, conn):
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

    async def revoke_worker(self, conn):
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
                INSERT INTO {table}
                    (worker_id, last_seen)
                VALUES (%s, NOW())
                """
            ).format(table=self.leadership_table),
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

    def __getitem__(self, key: str):
        return self.queues_by_name[key]

    def __contains__(self, key: str):
        return key in self.queues_by_name

    def __iter__(self):
        return iter(self.chancy.queues)

    def __len__(self):
        return len(self.chancy.queues)

    def __repr__(self):
        return f"<Worker({self.worker_id!r})>"

    @cached_property
    def queues_by_name(self):
        return {q.name: q for q in self.chancy.queues}
