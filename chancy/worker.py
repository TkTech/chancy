import re
import asyncio
import json
import uuid
import socket
import platform

from psycopg import sql
from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg.types.json import Json

from chancy.app import Chancy
from chancy.hub import Hub
from chancy.queue import Queue
from chancy.utils import TaskManager, import_string
from chancy.job import JobInstance


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
    :param queue_change_poll_interval: The number of seconds between checks for
                                       changes to the queues that the worker
                                       should be processing.
    :param shutdown_timeout: The number of seconds to wait for a clean shutdown
                                before forcing the worker to stop.
    """

    def __init__(
        self,
        chancy: Chancy,
        *,
        worker_id: str | None = None,
        heartbeat_poll_interval: int = 30,
        heartbeat_timeout: int = 90,
        queue_change_poll_interval: int = 10,
        shutdown_timeout: int = 30,
        tags: set[str] | None = None,
    ):
        #: The Chancy application that the worker is associated with.
        self.chancy = chancy
        #: The ID of the worker, which must be globally unique.
        self.worker_id = worker_id or str(uuid.uuid4())
        #: The number of seconds between heartbeat poll intervals.
        self.heartbeat_poll_interval = heartbeat_poll_interval
        #: The number of seconds before a worker is considered to have lost
        #: connection to the cluster.
        self.heartbeat_timeout = heartbeat_timeout
        #: The number of seconds to wait for a clean shutdown before forcing
        #: the worker to stop.
        self.shutdown_timeout = shutdown_timeout
        #: The number of seconds between sending outgoing updates to the
        #: database.
        self.send_outgoing_interval = 1
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
        self.outgoing = asyncio.Queue()

    async def start(self):
        """
        Start the worker.

        Will run indefinitely, polling the queues for new jobs and running any
        configured plugins.
        """
        self.manager.add(self._maintain_queues(), name="queues")
        self.manager.add(self._maintain_updates(), name="updates")
        self.manager.add(self._maintain_heartbeat(), name="heartbeat")

        # pgbouncer and other connection pools may not support LISTEN/NOTIFY
        # properly, so this needs to remain an optional (but default) feature.
        if self.chancy.notifications:
            self.manager.add(
                self._maintain_notifications(), name="notifications"
            )

        for plugin in self.chancy.plugins:
            await self.add_plugin(plugin)
            self.chancy.log.info(
                f"Started plugin {plugin.__class__.__name__!r}"
            )

        try:
            await self.manager.run(logger=self.chancy.log)
        except asyncio.CancelledError:
            clean = await self.manager.shutdown(timeout=self.shutdown_timeout)
            if not clean:
                self.chancy.log.error(
                    f"Failed to shutdown worker tasks within"
                    f" {self.shutdown_timeout} seconds."
                )

    async def add_plugin(self, plugin):
        """
        Add a plugin to the worker.

        :param plugin: The plugin to add.
        """
        self.manager.add(
            plugin.run(self, self.chancy),
            name=plugin.__class__.__name__,
        )

    async def _maintain_queues(self):
        """
        Maintain the queues that the worker is processing.

        This will periodically check the database for any changes to the queues
        that the worker should be processing, and update the worker's queues
        accordingly.
        """
        while True:
            self.chancy.log.debug("Polling for queue changes.")
            async with self.chancy.pool.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cursor:
                    tags = self.worker_tags()
                    await cursor.execute(
                        sql.SQL(
                            """
                            SELECT 
                                name,
                                concurrency,
                                tags,
                                state,
                                executor,
                                executor_options,
                                polling_interval
                            FROM {queues}
                        """
                        ).format(
                            queues=sql.Identifier(f"{self.chancy.prefix}queues")
                        ),
                    )
                    # Tags in the database is a list of regexes, while the tags
                    # in the worker are a set of strings. We need to filter the
                    # queues based on the worker's tags.
                    db_queues = {
                        q["name"]: Queue(**q)
                        for q in await cursor.fetchall()
                        if any(
                            re.match(t, tag) for tag in tags for t in q["tags"]
                        )
                    }

            # Remove queues that are no longer in the database
            for queue_name in list(self._queues.keys()):
                if queue_name not in db_queues:
                    del self._queues[queue_name]
                    self.chancy.log.info(f"Removed queue {queue_name}")

            # Add or update queues
            for queue_name, queue in db_queues.items():
                if queue_name not in self._queues:
                    self._queues[queue_name] = queue
                    self.manager.add(
                        self._maintain_queue(queue_name),
                        name=f"queue_{queue_name}",
                    )
                    self.chancy.log.info(f"Added queue {queue_name}")
                else:
                    # Update existing queue if necessary
                    if self._queues[queue_name] != queue:
                        self._queues[queue_name] = queue
                        self.chancy.log.info(f"Updated queue {queue_name}")

            await asyncio.sleep(self.queue_change_poll_interval)

    async def _maintain_queue(self, queue_name: str):
        """
        Maintain a single queue.

        :param queue_name: The queue to maintain.
        """
        queue = self._queues[queue_name]
        executor = import_string(queue.executor)(
            self, queue, **queue.executor_options
        )

        while True:
            try:
                queue = self._queues[queue_name]
            except KeyError:
                self.chancy.log.info(
                    f"Queue {queue_name!r} no longer exists, stopping its"
                    f" maintenance loop."
                )
                return

            maximum_jobs_to_poll = queue.concurrency - len(executor)
            if maximum_jobs_to_poll <= 0:
                await asyncio.sleep(queue.polling_interval)
                continue

            if queue.state == Queue.State.PAUSED:
                self.chancy.log.debug(
                    f"Queue {queue.name!r} is paused, skipping polling."
                )
                await asyncio.sleep(queue.polling_interval)
                continue

            async with self.chancy.pool.connection() as conn:
                jobs = await self.fetch_jobs(
                    queue, conn, up_to=maximum_jobs_to_poll
                )

            await self.hub.emit(
                "queue.polled",
                {
                    "fetched": len(jobs),
                    "worker_id": self.worker_id,
                    "queue": queue.name,
                },
            )

            for job in jobs:
                self.chancy.log.debug(
                    f"Found job {job.id}, pushing to executor."
                )
                await executor.push(job)

            await asyncio.sleep(queue.polling_interval)

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
                    self.chancy.log.debug("Announcing worker to the cluster.")
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
        self.chancy.log.info("Started listening for realtime notifications.")
        async for notification in connection.notifies():
            j = json.loads(notification.payload)
            event = j.pop("t")
            await self.hub.emit(event, j)

    async def _maintain_updates(self):
        """
        Process updates to job instances.

        We maintain a queue of updates to job instances, and process them in
        batches to significantly reduce the number of transactions that need
        to be made.

        .. note::

            This method should not be called directly. It is automatically
            run when the worker is started.
        """
        while True:
            if self.outgoing.empty():
                await asyncio.sleep(self.send_outgoing_interval)
                continue

            pending_updates = []
            while len(pending_updates) < 1000:
                try:
                    pending_updates.append(self.outgoing.get_nowait())
                except asyncio.QueueEmpty:
                    break

            async with self.chancy.pool.connection() as conn:
                async with conn.cursor() as cursor:
                    async with conn.transaction():
                        try:
                            await cursor.executemany(
                                sql.SQL(
                                    """
                                    UPDATE
                                        {jobs}
                                    SET
                                        state = %(state)s,
                                        started_at = %(started_at)s,
                                        completed_at = %(completed_at)s,
                                        attempts = %(attempts)s,
                                        errors = %(errors)s
                                    WHERE
                                        id = %(id)s
                                    """
                                ).format(
                                    jobs=sql.Identifier(
                                        f"{self.chancy.prefix}jobs"
                                    )
                                ),
                                [
                                    {
                                        "id": update.id,
                                        "state": update.state.value,
                                        "started_at": update.started_at,
                                        "completed_at": update.completed_at,
                                        "attempts": update.attempts,
                                        "errors": Json(update.errors),
                                    }
                                    for update in pending_updates
                                ],
                            )
                        except Exception:
                            # If we were unable to apply the updates, we should
                            # re-queue them for the next poll.
                            for update in pending_updates:
                                await self.outgoing.put(update)
                            raise

            await asyncio.sleep(self.send_outgoing_interval)

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

    async def push_update(self, update: JobInstance):
        """
        Push an update to the job instance.

        .. note::

            This will not immediately update the job instance in the database,
            but will instead queue the update to be processed.

        :param update: The job instance to update.
        """
        await self.outgoing.put(update)

    async def fetch_jobs(
        self,
        queue: Queue,
        conn: AsyncConnection,
        *,
        up_to: int = 1,
    ) -> list[JobInstance]:
        """
        Fetch jobs from the queue.

        This method will fetch up to `up_to` jobs from the queue, mark them as
        running, and return them as a list of `JobInstance` objects.

        It's safe to call this method concurrently, as the jobs will be locked
        for the duration of the transaction.

        :param queue: The queue to fetch jobs from.
        :param conn: The database connection to use.
        :param up_to: The maximum number of jobs to fetch.
        """
        jobs_table = sql.Identifier(f"{self.chancy.prefix}jobs")

        async with conn.cursor(row_factory=dict_row) as cursor:
            async with conn.transaction():
                await cursor.execute(
                    sql.SQL(
                        """
                        with selected_jobs as (
                            select
                                id
                            from
                                {jobs}
                            where
                                queue = %(queue)s
                            and
                                (state = 'pending' or state = 'retrying')
                            and
                                attempts < max_attempts
                            and
                                (scheduled_at is null or scheduled_at <= now())
                            order by
                                priority asc,
                                id desc
                            limit
                                %(maximum_jobs_to_fetch)s
                            for update of {jobs} skip locked
                        )
                        update
                            {jobs}
                        set
                            started_at = now(),
                            state = 'running',
                            taken_by = %(worker_id)s
                        from
                            selected_jobs
                        where
                            {jobs}.id = selected_jobs.id
                        returning {jobs}.*
                        """
                    ).format(
                        jobs=jobs_table,
                    ),
                    {
                        "queue": queue.name,
                        "maximum_jobs_to_fetch": up_to,
                        "worker_id": self.worker_id,
                    },
                )

                records = await cursor.fetchall()

            return [JobInstance.unpack(record) for record in records]

    def __repr__(self):
        return f"<Worker({self.worker_id!r})>"
