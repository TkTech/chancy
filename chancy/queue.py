import asyncio
import json
from typing import TYPE_CHECKING, Any
from functools import cached_property

from psycopg import AsyncConnection, sql, AsyncCursor
from psycopg.rows import dict_row
from psycopg.types.json import Json

from chancy.executor import Executor
from chancy.job import Job, Reference, JobInstance
from chancy.utils import chancy_uuid, chunked, import_string
from chancy.plugin import Plugin, PluginScope

if TYPE_CHECKING:
    from chancy.app import Chancy
    from chancy.worker import Worker


class Queue(Plugin):
    """
    A postgres-backed queue that uses the database to store jobs.
    """

    class QueueState:
        """
        The state of the queue.
        """

        #: The queue is active and can process jobs.
        ACTIVE = "active"
        #: The queue is paused and will not process jobs.
        PAUSED = "paused"

    @classmethod
    def get_scope(cls) -> PluginScope:
        return PluginScope.WORKER

    def __init__(
        self,
        name: str,
        *,
        state: QueueState = QueueState.ACTIVE,
        concurrency: int = 1,
        executor: str | None = None,
        executor_options: dict[str, Any] | None = None,
        polling_interval: int = 1,
        tags: set | None = None,
    ):
        super().__init__()
        #: The name of the queue.
        self.name = name
        #: The state of the queue.
        self.state = state
        #: The number of jobs that can be processed concurrently per worker.
        self.concurrency = concurrency
        #: The executor to use for processing jobs in this queue.
        self._executor = executor or "chancy.executors.process.ProcessExecutor"
        #: Options to pass to the executor.
        self._executor_options = executor_options or {}
        #: The number of seconds to wait between polling the queue for new jobs.
        self.polling_interval = polling_interval or 1
        # A queue of pending updates to jobs that need to be applied on the
        # next fetch.
        self.pending_updates = asyncio.Queue()
        # Only workers that match these tags will actively process this queue.
        self.tags = tags or {"*"}

    @cached_property
    def executor(self) -> Executor:
        return import_string(self._executor)(self, **self._executor_options)

    async def run(self, worker: "Worker", app: "Chancy"):
        """
        Continuously polls the queue for new jobs.

        This method will run indefinitely, polling the queue for new jobs and
        running them as they become available.

        .. note::

            If you want to pull jobs from the queue to process them yourself,
            you can use the :meth:`fetch()` method directly.

        :param app: The app that is polling the queue.
        :param worker: The worker that is polling the queue.
        """
        # Skip the first sleep, as we want to start polling immediately.
        self.wakeup_signal.set()
        while await self.sleep(self.polling_interval):
            async with app.pool.connection() as conn:
                async with conn.cursor() as cursor:
                    await self.declare(app, cursor)

                # If we wouldn't be able to run a job even if we had one, we
                # should just wait. Pre-fetching can be advantageous, but
                # IMO it causes more headache (as seen with Celery and future
                # scheduled tasks) than it's worth.
                maximum_jobs_to_poll = self.concurrency - len(self.executor)
                if maximum_jobs_to_poll <= 0:
                    self.log.debug("No capacity for new jobs, skipping poll.")
                    await asyncio.sleep(self.polling_interval)
                    continue

                jobs = await self.fetch_jobs(
                    conn,
                    up_to=maximum_jobs_to_poll,
                    prefix=app.prefix,
                    worker_id=worker.worker_id,
                )

                await worker.hub.emit(
                    "queue.polled",
                    {
                        "fetched": len(jobs),
                        "worker_id": worker.worker_id,
                        "queue": self.name,
                    },
                )

                for job in jobs:
                    self.log.debug(f"Found job {job.id}, pushing to executor.")
                    await self.executor.push(job)

    async def fetch_jobs(
        self,
        conn: AsyncConnection,
        *,
        up_to: int = 1,
        prefix: str = "chancy_",
        worker_id: str | None = None,
    ) -> list[JobInstance]:
        """
        Fetch jobs from the queue.

        This method will fetch up to `up_to` jobs from the queue, mark them as
        running, and return them as a list of `JobInstance` objects.

        It's safe to call this method concurrently, as the jobs will be locked
        for the duration of the transaction.

        :param conn: The database connection to use.
        :param up_to: The maximum number of jobs to fetch.
        :param prefix: The prefix to use for the database tables.
        :param worker_id: The ID of the worker fetching the jobs.
        """
        jobs_table = sql.Identifier(f"{prefix}jobs")

        async with conn.cursor(row_factory=dict_row) as cursor:
            async with conn.transaction():
                if not self.pending_updates.empty():
                    # Gather all pending updates and apply them to the jobs
                    # table before fetching new jobs.
                    pending_updates = []
                    while not self.pending_updates.empty():
                        pending_updates.append(await self.pending_updates.get())

                    try:
                        await cursor.executemany(
                            sql.SQL(
                                """
                                UPDATE
                                    {jobs}
                                SET
                                    state = %(state)s,
                                    completed_at = %(completed_at)s,
                                    attempts = %(attempts)s
                                WHERE
                                    id = %(id)s
                                """
                            ).format(jobs=jobs_table),
                            [
                                {
                                    "id": update.id,
                                    "state": update.state,
                                    "completed_at": update.completed_at,
                                    "attempts": update.attempts,
                                }
                                for update in pending_updates
                            ],
                        )
                    except Exception:
                        # If we were unable to apply the updates, we should
                        # re-queue them for the next poll.
                        for update in pending_updates:
                            await self.pending_updates.put(update)
                        raise

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
                                (state = 'pending' or state = 'retrying')
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
                            attempts = attempts + 1,
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
                        "queue": self.name,
                        "maximum_jobs_to_fetch": up_to,
                        "worker_id": worker_id,
                    },
                )

                records = await cursor.fetchall()

            return [JobInstance.unpack(record) for record in records]

    async def push_job_update(self, job: JobInstance):
        """
        Push an update to a job.

        This method is typically called by an executor after a job has been
        completed (either successfully or with an error) to update the job's
        state in the database. It does _not_ immediately update the job,
        instead they are batched and updated the next time the worker polls
        the queue.
        """
        await self.pending_updates.put(job)

    async def push(self, app: "Chancy", jobs: list[Job]) -> list[Reference]:
        async with app.pool.connection() as conn:
            async with conn.cursor() as cursor:
                # Excessive numbers of jobs will cause the transaction to throw
                # memory errors, so we need to chunk them.
                for chunk in chunked(jobs, 1000):
                    async with conn.transaction():
                        return await self.push_jobs(
                            cursor,
                            list(chunk),
                            prefix=app.prefix,
                            notifications=app.notifications,
                        )

    async def push_jobs(
        self,
        cursor: AsyncCursor,
        jobs: list[Job],
        *,
        prefix: str = "chancy_",
        notifications: bool = True,
    ) -> list[Reference]:
        """
        Push one or more jobs onto the queue.

        This advanced method can be used to push jobs to the database
        within an existing transaction. This is very useful when you want to
        push jobs to the queue only if related objects are successfully
        created, like running onboarding for a new user only if the user is
        successfully created.

        :param cursor: The database cursor to use.
        :param jobs: The jobs to push onto the queue.
        :param prefix: The prefix to use for the database tables.
        :param notifications: Whether to notify the cluster of the new jobs.
        """

        # We used to use a single executemany() here, but switched to doing
        # several inserts to better support the RETURNING clause with
        # unique jobs. This allows us to return a reference to the existing
        # job when a conflict occurs, trading performance for convenience.
        references = []
        for job in jobs:
            await cursor.execute(
                # The DO UPDATE clause is used to trick Postgres into returning
                # the existing ID. Otherwise, since no row was touched, the
                # RETURNING clause would get ignored. This isn't ideal - it
                # causes an unnecessary write, but in any normal usage this
                # should be a non-issue.
                sql.SQL(
                    """
                    INSERT INTO
                        {jobs} (
                            id,
                            queue,
                            payload,
                            priority,
                            max_attempts,
                            scheduled_at,
                            unique_key
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (unique_key)
                    WHERE
                        unique_key IS NOT NULL
                            AND state NOT IN ('succeeded', 'failed')
                    DO UPDATE
                       SET
                           state = EXCLUDED.state
                    RETURNING id;
                    """
                ).format(
                    jobs=sql.Identifier(f"{prefix}jobs"),
                ),
                (
                    chancy_uuid(),
                    self.name,
                    Json(
                        {
                            "func": job.func,
                            "kwargs": job.kwargs or {},
                            "limits": [
                                limit.serialize() for limit in job.limits
                            ],
                        }
                    ),
                    job.priority,
                    job.max_attempts,
                    job.scheduled_at,
                    job.unique_key,
                ),
            )
            record = await cursor.fetchone()
            references.append(Reference(self, record[0]))

        # Notify the cluster that new jobs have been pushed, allowing workers
        # to wake up and start processing them immediately.
        if notifications:
            await cursor.execute(
                sql.SQL("SELECT pg_notify({events}, {event});").format(
                    events=sql.Literal(f"{prefix}events"),
                    event=sql.Literal(
                        json.dumps({"t": "pushed", "q": self.name})
                    ),
                )
            )

        return references

    async def get_job(self, app: "Chancy", ref: Reference) -> JobInstance:
        """
        Fetch a single job from the database.
        """
        async with app.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    sql.SQL(
                        """
                        SELECT
                            *
                        FROM
                            {jobs}
                        WHERE
                            id = %s
                        """
                    ).format(jobs=sql.Identifier(f"{app.prefix}jobs")),
                    [ref.identifier],
                )
                record = await cursor.fetchone()
                if record is None:
                    raise KeyError(f"Job {ref.identifier} not found.")

                return JobInstance.unpack(record)

    async def declare(
        self, app: "Chancy", cursor: AsyncCursor, *, upsert: bool = False
    ):
        """
        Declare the queue to the cluster.

        Chancy's Queues are global, meaning that every worker can see every
        available queue. Worker tags are used to determine which workers will
        process which queues, such as `has-gpu=true` or `hostname=worker1`.

        By default, if a queue with the same name already existed in the
        database, the settings in the database will overwrite the settings
        of the queue being declared. Passing `upsert=True` will instead
        update the settings of the queue in the database with the settings
        of the queue being declared.
        """
        action = sql.SQL(
            """
            UPDATE SET
                name = EXCLUDED.name
            """
        )
        if upsert:
            action = sql.SQL(
                """
                UPDATE SET
                    state = EXCLUDED.state,
                    concurrency = EXCLUDED.concurrency,
                    tags = EXCLUDED.tags,
                    executor = EXCLUDED.executor,
                    executor_options = EXCLUDED.executor_options,
                    polling_interval = EXCLUDED.polling_interval
                """
            )

        await cursor.execute(
            sql.SQL(
                """
                INSERT INTO {queues} (
                    name,
                    state,
                    concurrency,
                    tags,
                    executor,
                    executor_options,
                    polling_interval
                ) VALUES (
                    %(name)s,
                    %(state)s,
                    %(concurrency)s,
                    %(tags)s,
                    %(executor)s,
                    %(executor_options)s,
                    %(polling_interval)s
                )
                ON CONFLICT (name) DO
                    {action}
                RETURNING (
                    state,
                    concurrency,
                    tags,
                    polling_interval,
                    executor,
                    executor_options
                )
                """
            ).format(
                queues=sql.Identifier(f"{app.prefix}queues"),
                action=action,
            ),
            {
                "name": self.name,
                "state": self.state,
                "concurrency": self.concurrency,
                "tags": list(self.tags),
                "executor": self._executor,
                "executor_options": Json(self._executor_options),
                "polling_interval": self.polling_interval,
            },
        )

        result = await cursor.fetchone()
        if result and not upsert:
            row = result[0]
            self.state = row[0]
            self.concurrency = int(row[1])
            self.tags = set(row[2])
            self.polling_interval = int(row[3])

            # Not well-supported yet, but it is in theory possible to hot-swap
            # the executor.
            if row[4] != self._executor:
                self._executor = row[4]
                self._executor_options = row[5]
                del self.executor
