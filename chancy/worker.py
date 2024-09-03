import re
import asyncio
import json
import time
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

    Starting a worker is simple:

    .. code-block:: python

        async with Chancy(
            dsn="postgresql://localhost/postgres",
        ) as chancy:
            await chancy.migrate()
            await Worker(chancy).start()

    Worker Tags
    -----------

    Worker tags control which workers run which queues. A tag is a string
    (typically in the format ``key=value``) assigned to a worker. Declare
    a queue with a list of tags, and only workers with matching tags will
    run that queue.

    Every worker automatically gets some tags, like ``hostname``, ``worker_id``,
    ``python``, ``arch``, ``os``, and ``*`` (wildcard). You can also add custom
    tags when creating a worker.

    .. code-block:: python

        await Worker(chancy, tags={"has=gpu", "has=large-disk"}).start()

    You could then assign a queue to only run on workers with the ``has=gpu`` tag:

    .. code-block:: python

        await chancy.declare(
            Queue(name="default", tags={"has=gpu"}, concurrency=10),
            upsert=True  # Replaces existing settings
        )

    Tags are regexes, allowing for flexible matching:

    .. code-block:: python

        Queue(name="default", tags={r"python=3\.11\.[0-9]+"}, concurrency=10)

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
            await self.manager.run()
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
                                polling_interval,
                                rate_limit,
                                rate_limit_window
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
                        q["name"]: Queue.unpack(q)
                        for q in await cursor.fetchall()
                        if any(
                            re.match(t, tag) for tag in tags for t in q["tags"]
                        )
                    }

            for queue_name in list(self._queues.keys()):
                if queue_name not in db_queues:
                    del self._queues[queue_name]
                    self.chancy.log.info(f"Removed queue {queue_name}")

            for queue_name, queue in db_queues.items():
                if queue_name not in self._queues:
                    self._queues[queue_name] = queue
                    self.manager.add(
                        self._maintain_queue(queue_name),
                        name=f"queue_{queue_name}",
                    )
                    self.chancy.log.info(
                        f"Adding queue {queue_name!r} to worker."
                    )
                else:
                    if self._queues[queue_name] != queue:
                        self._queues[queue_name] = queue
                        self.chancy.log.info(
                            f"Updated configuration for the queue"
                            f" {queue_name!r}."
                        )

            await asyncio.sleep(self.queue_change_poll_interval)

    async def _maintain_queue(self, queue_name: str):
        """
        Maintain a single queue.

        Responsible for polling a queue for new jobs and pushing them to the
        executor.

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
        react to cluster events using Postgres's LISTEN/NOTIFY feature.

        .. note::

            This feature utilizes a permanent connection to the database
            separate from the shared connection pool and is not counted against
            the pool's connection limit.
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
            await self.hub.emit(j.pop("t"), j)

    async def _maintain_updates(self):
        """
        Process updates to job instances.

        We maintain a queue of updates to job instances, and process them in
        batches to significantly reduce the number of overall transactions that
        need to be made, at the cost of potentially losing some updates if the
        worker is stopped unexpectedly. The frequency of these updates can be
        controlled by setting the `send_outgoing_interval` attribute on the
        worker.
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

    async def queue_update(self, update: JobInstance):
        """
        Enqueue an update to a job instance.

        This method will queue an update to a job instance to be processed in
        periodic batches, reducing the number of transactions that need to be
        made. You can control the frequency of these updates by setting the
        `send_outgoing_interval` attribute on the worker.

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
        Fetch jobs from the queue for processing.

        This method will fetch up to `up_to` jobs from the queue, mark them as
        running, and return them as a list of `JobInstance` objects. If no jobs
        are available, an empty list will be returned.

        It's safe to call this method concurrently, as the jobs will be locked
        for the duration of the transaction.

        :param queue: The queue to fetch jobs from.
        :param conn: The database connection to use.
        :param up_to: The maximum number of jobs to fetch.
        """
        jobs_table = sql.Identifier(f"{self.chancy.prefix}jobs")
        rate_limits_table = sql.Identifier(
            f"{self.chancy.prefix}queue_rate_limits"
        )

        async with conn.cursor(row_factory=dict_row) as cursor:
            async with conn.transaction():
                # If the queue is configured to use a rate limit, we need to
                # check if there's any remaining capacity in the current
                # window.
                if queue.rate_limit:
                    now = int(time.time())
                    window_start = now - (now % queue.rate_limit_window)

                    await cursor.execute(
                        sql.SQL(
                            """
                            INSERT INTO {rate_limits_table} (
                                queue,
                                window_start,
                                count
                            )
                            VALUES (%s, %s, 0)
                            ON CONFLICT (queue) DO UPDATE
                            SET 
                                count = CASE
                                    WHEN {rate_limits_table}.window_start
                                        = EXCLUDED.window_start
                                            THEN {rate_limits_table}.count
                                    ELSE 0
                                END,
                                window_start = EXCLUDED.window_start
                            RETURNING count
                            """
                        ).format(rate_limits_table=rate_limits_table),
                        (queue.name, window_start),
                    )

                    result = await cursor.fetchone()
                    current_count = result["count"]

                    # If we've hit the rate limit, return early with no jobs
                    # fetched.
                    if current_count >= queue.rate_limit:
                        return []

                    # Adjust up_to based on remaining rate limit
                    up_to = min(up_to, queue.rate_limit - current_count)

                await cursor.execute(
                    sql.SQL(
                        """
                        WITH selected_jobs AS (
                            SELECT
                                id
                            FROM
                                {jobs}
                            WHERE
                                queue = %(queue)s
                            AND
                                (state = 'pending' OR state = 'retrying')
                            AND
                                attempts < max_attempts
                            AND
                                (scheduled_at IS NULL OR scheduled_at <= NOW())
                            ORDER BY
                                priority ASC,
                                id DESC
                            LIMIT
                                %(maximum_jobs_to_fetch)s
                            FOR UPDATE OF {jobs} SKIP LOCKED
                        )
                        UPDATE
                            {jobs}
                        SET
                            started_at = NOW(),
                            state = 'running',
                            taken_by = %(worker_id)s
                        FROM
                            selected_jobs
                        WHERE
                            {jobs}.id = selected_jobs.id
                        RETURNING {jobs}.*
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

                # If a rate limit is configured, and we ended up fetching jobs,
                # we need to increment the rate limit counter.
                if queue.rate_limit and records:
                    await cursor.execute(
                        sql.SQL(
                            """
                            UPDATE {rate_limits_table}
                            SET count = count + %s
                            WHERE queue = %s
                            """
                        ).format(rate_limits_table=rate_limits_table),
                        (len(records), queue.name),
                    )

            return [JobInstance.unpack(record) for record in records]

    def __repr__(self):
        return f"<Worker({self.worker_id!r})>"
