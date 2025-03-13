import re
import asyncio
import json
import time
import uuid
import socket
import signal
import platform

import sys
import warnings

from psycopg import sql
from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg.types.json import Json

from chancy.app import Chancy
from chancy.executors.base import Executor
from chancy.hub import Hub, Event
from chancy.queue import Queue
from chancy.utils import TaskManager, import_string
from chancy.job import QueuedJob, Reference


class Worker:
    """
    The Worker is responsible for polling queues for new jobs, running any
    configured plugins, and internal management such as heartbeats.

    Starting a worker is simple:

    .. code-block:: python

        async with Chancy(
            "postgresql://localhost/postgres",
        ) as chancy:
            await chancy.migrate()
            async with Worker(chancy) as worker:
                await worker.wait_for_shutdown()

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

        async with Worker(chancy, tags={"has=gpu", "has=large-disk"}) as worker:
            await worker.wait_for_shutdown()

    You could then assign a queue to only run on workers with the ``has=gpu`` tag:

    .. code-block:: python

        await chancy.declare(
            Queue(name="default", tags={"has=gpu"}, concurrency=10),
            upsert=True  # Replaces existing settings
        )

    Tags are regexes, allowing for flexible matching:

    .. code-block:: python

        Queue(name="default", tags={r"python=3\\.11\\.[0-9]+"}, concurrency=10)

    Events
    ------

    The worker emits events that can be listened to by plugins. The following
    events are emitted:

    .. list-table::
        :header-rows: 1

        * - Event
          - Kwargs
          - Description
        * - worker.started
          - worker
          - Emitted when the worker is started.
        * - worker.stopped
          - worker
          - Emitted when the worker is stopped.
        * - worker.shutdown_timeout
          - worker
          - Emitted when the worker encounters a timeout while trying to
            shut down.
        * - worker.queue.started
          - queue, executor, worker
          - Emitted when a queue is started.
        * - worker.queue.removed
          - queue, executor, worker
          - Emitted when a queue is removed.
        * - worker.queue.updated
          - queue, executor, worker
          - Emitted when a queue is updated.

    :param chancy: The Chancy application.
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
    :param tags: Extra tags to associate with the worker.
    :param register_signal_handlers: Whether to register signal handlers.
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
        register_signal_handlers: bool = True,
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
        #: Whether to register signal handlers on startup of the worker.
        self.register_signal_handlers = register_signal_handlers
        # Extra tags to associate with the worker.
        self._extra_tags = tags or set()

        #: The task manager for the worker, responsible for managing all the
        #: worker's internal asyncio tasks.
        self.manager = TaskManager()

        #: A queue of updates waiting to be sent to the database.
        self.outgoing: asyncio.Queue[QueuedJob] = asyncio.Queue()

        #: An event that is set when the worker is the leader.
        #: This functionality is not enabled by default - a leadership plugin
        #: must be used to enable this event.
        self.is_leader = asyncio.Event()
        #: An event that is set when the worker is shutting down due to
        #: receiving a signal.
        self.shutdown_event = asyncio.Event()

        # Set once the notifications listener is ready.
        self._notifications_ready_event = asyncio.Event()
        # The queues that the worker is currently processing.
        self._queues: dict[str, Queue] = {}
        # The executors that the worker is currently using.
        self._executors: dict[str, Executor] = {}

    async def start(self):
        """
        Start the worker and begin processing jobs.

        This method will return immediately - to wait until the worker is
        stopped, use the `wait_for_shutdown` method:

        .. code-block:: python

            worker = Worker(chancy)
            await worker.start()
            await worker.wait_for_shutdown()

        If possible, it's better to use the worker as an async context manager
        to ensure that it's properly cleaned up:

        .. code-block:: python

            async with Worker(chancy) as worker:
                await worker.wait_for_shutdown()
        """
        self.hub.on("job.cancelled", self._handle_cancellation)

        for plugin in self.chancy.plugins:
            self.manager.add(
                plugin.__class__.__name__,
                plugin.run(self, self.chancy),
            )
            self.chancy.log.info(
                f"Started plugin {plugin.__class__.__name__!r}"
            )

        self.manager.add("queues", self._maintain_queues())
        self.manager.add("updates", self._maintain_updates())
        self.manager.add("heartbeat", self._maintain_heartbeat())

        # pgbouncer and other connection pools may not support LISTEN/NOTIFY
        # properly, so this needs to remain an optional (but default) feature.
        if self.chancy.notifications:
            self.manager.add("notifications", self._maintain_notifications())
            await self._notifications_ready_event.wait()

        if self.register_signal_handlers and sys.platform == "win32":
            warnings.warn(
                "Signal handlers are not supported on Windows, "
                "use CTRL+C to stop the worker.",
                RuntimeWarning,
            )
        elif self.register_signal_handlers:
            loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
            for sig in {signal.SIGTERM, signal.SIGINT}:
                loop.add_signal_handler(
                    sig,
                    lambda: asyncio.create_task(self.on_signal(sig)),
                )

        await self.chancy.declare(Queue(name="default"))
        await self.hub.emit(
            "worker.started",
            {
                "worker": self,
            },
        )

    async def wait_for_shutdown(self):
        """
        Wait until the worker is stopped.
        """
        await self.manager.wait_for_shutdown()

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
                        f"queue_{queue_name}", self._maintain_queue(queue)
                    )
                    self.chancy.log.info(
                        f"Adding queue {queue_name!r} to worker using executor"
                        f" {queue.executor!r}."
                    )
                else:
                    if self._queues[queue_name] != queue:
                        self._queues[queue_name] = queue
                        self.chancy.log.info(
                            f"Updated configuration for the queue"
                            f" {queue_name!r}."
                        )

            try:
                await self.hub.wait_for(
                    "queue.declared",
                    timeout=self.queue_change_poll_interval,
                )
            except asyncio.TimeoutError:
                pass

    async def _maintain_queue(self, queue: Queue):
        """
        Maintain a single queue.

        Responsible for polling a queue for new jobs and pushing them to the
        executor.

        :param queue: The queue to maintain.
        """
        while queue := self._queues.get(queue.name):
            cls = import_string(queue.executor)

            async with cls(self, queue, **queue.executor_options) as executor:
                try:
                    self._executors[queue.name] = executor

                    concurrency = queue.concurrency
                    if queue.concurrency is None:
                        concurrency = executor.get_default_concurrency()

                    await self.hub.emit(
                        "worker.queue.started",
                        {
                            "queue": queue,
                            "executor": executor,
                            "worker": self,
                        },
                    )

                    while True:
                        new_queue = self._queues.get(queue.name)

                        if new_queue != queue:
                            # Drain the queue if it's being removed / updated
                            while len(executor) > 0:
                                await asyncio.sleep(queue.polling_interval)

                            # The queue was completely removed.
                            if new_queue is None:
                                await self.hub.emit(
                                    "worker.queue.removed",
                                    {
                                        "queue": queue,
                                        "executor": executor,
                                        "worker": self,
                                    },
                                )
                                return

                            # Otherwise it's just a configuration change, so
                            # break out to the outer loop to reconfigure the
                            # executor.
                            await self.hub.emit(
                                "worker.queue.updated",
                                {
                                    "queue": queue,
                                    "executor": executor,
                                    "worker": self,
                                },
                            )
                            break

                        if queue.state == Queue.State.PAUSED:
                            await asyncio.sleep(queue.polling_interval)
                            continue

                        maximum_jobs_to_poll = concurrency - len(executor)
                        if maximum_jobs_to_poll <= 0:
                            await asyncio.sleep(queue.polling_interval)
                            continue

                        async with self.chancy.pool.connection() as conn:
                            jobs = await self.fetch_jobs(
                                queue, conn, up_to=maximum_jobs_to_poll
                            )

                        for job in jobs:
                            self.chancy.log.info(
                                f"Pulled {job.id!r} ({job.func!r}) for queue"
                                f"{job.queue!r}"
                            )
                            await executor.push(job)

                        await asyncio.sleep(queue.polling_interval)
                finally:
                    self._executors.pop(queue.name, None)

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
        self._notifications_ready_event.set()
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

            self.chancy.log.debug(
                f"Processing {len(pending_updates)} outgoing updates."
            )

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
                                        scheduled_at = %(scheduled_at)s,
                                        attempts = %(attempts)s,
                                        errors = %(errors)s,
                                        meta = %(meta)s,
                                        max_attempts = %(max_attempts)s
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
                                        "scheduled_at": update.scheduled_at,
                                        "attempts": update.attempts,
                                        "errors": Json(update.errors),
                                        "meta": Json(update.meta),
                                        "max_attempts": update.max_attempts,
                                    }
                                    for update in pending_updates
                                ],
                            )
                        except Exception:
                            # If we were unable to apply the updates, we should
                            # re-queue them for the next poll.
                            self.chancy.log.exception(
                                "Failed to apply updates to job instances."
                            )
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

    async def queue_update(self, update: QueuedJob):
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
    ) -> list[QueuedJob]:
        """
        Fetch jobs from the queue for processing.

        This method will fetch up to `up_to` jobs from the queue, mark them as
        running, and return them as a list of `QueuedJob` objects. If no jobs
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
                                priority DESC,
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

                return [QueuedJob.unpack(record) for record in records]

    async def on_signal(self, signum: int):
        """
        Called when a signal is received.

        By default, handles SIGTERM and SIGINT by starting worker shutdown. If
        the signal is received again, it will force a shutdown.
        """
        if signum in (signal.SIGTERM, signal.SIGINT):
            # If the worker is already shutting down, we force an immediate
            # shutdown.
            if self.shutdown_event.is_set():
                self.chancy.log.warning(
                    "Received signal again, forcing shutdown."
                )
                asyncio.get_running_loop().stop()

            self.shutdown_event.set()
            await self.manager.cancel_all()

    async def stop(self) -> bool:
        """
        Stop the worker.

        Attempts to stop the worker gracefully, sending a CancelledError to all
        running tasks and waiting up to `shutdown_timeout` seconds for them to
        complete before returning.

        Returns True if the worker was stopped cleanly, or False if the worker
        returned due to the timeout expiring.
        """
        try:
            async with asyncio.timeout(self.shutdown_timeout) as cm:
                # Stop accepting new queues and queue changes.
                try:
                    await self.manager.cancel("queues")
                except KeyError:
                    pass
                # Delete all the queues we know about so the executors can
                # clean up.
                self._queues.clear()
                while self._executors:
                    await asyncio.sleep(0.1)
                # And finally axe everything else started by this worker.
                await self.manager.cancel_all()
        except TimeoutError:
            # We check this instead of depending on the exception in case the
            # exception wasn't really raised by us but a nested timeout.
            if cm.expired():
                await self.hub.emit(
                    "worker.shutdown_timeout",
                    {
                        "worker": self,
                    },
                )
                return False
            raise

        await self.hub.emit(
            "worker.stopped",
            {
                "worker": self,
            },
        )
        return True

    async def _handle_cancellation(self, event: Event):
        self.chancy.log.info(
            f"Received cancellation request for job {event.body['j']!r}"
        )
        for executor in self._executors.values():
            await executor.cancel(Reference(event.body["j"]))

    @property
    def executors(self) -> dict[str, Executor]:
        """
        Get the executors that the worker is currently using to service queues.

        :return: A dictionary of executors keyed by queue name.
        """
        return self._executors

    def __repr__(self):
        return f"<Worker({self.worker_id!r})>"

    def __iter__(self):
        return iter(self._queues.values())

    def __getitem__(self, key):
        return self._queues[key]

    def __contains__(self, key):
        return key in self._queues

    def __len__(self):
        return len(self._queues)

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *exc):
        await self.stop()
