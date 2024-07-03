import json
import asyncio
from typing import Callable

from psycopg import sql, AsyncConnection
from psycopg.rows import dict_row
from psycopg.types.json import Json

from chancy.executor import Executor, JobInstance, Limit, Job
from chancy.executors.process import ProcessExecutor
from chancy.logger import PrefixAdapter, logger


class Queue:
    """
    A Chancy queue.

    Queues are used to store jobs that need to be run by workers. Each queue
    has a name, a concurrency limit, and an executor that is responsible for
    running the jobs in the queue.

    By default, a Queue will use a ProcessExecutor that is "good enough" for
    most use cases.

    :param name: The name of the queue.
    :param concurrency: The maximum number of jobs that can be run concurrently
                        by each worker on this queue.
    :param executor: The executor to use for running jobs.
    :param polling_interval: The interval at which to poll the queue for new
                             jobs.
    """

    def __init__(
        self,
        name: str,
        *,
        concurrency: int = 1,
        executor: Callable[["Queue"], Executor] | None = None,
        polling_interval: int | None = 5,
    ):
        self.name = name
        self.concurrency = concurrency
        self.polling_interval = polling_interval

        if executor is None:
            self.executor = ProcessExecutor(self)
        else:
            self.executor = executor(self)

        # A queue of pending updates to jobs that need to be applied on the
        # next fetch.
        self.pending_updates = asyncio.Queue()
        # An event that can be used to wake up the queue if it's sleeping.
        self._wake_up = asyncio.Event()

    async def poll(self, app, worker):
        """
        Continuously polls the queue for new jobs.

        This method will run indefinitely, polling the queue for new jobs and
        running them as they become available.

        If you want to pull jobs from the queue without further processing,
        you can use the `fetch_jobs` method directly instead.

        :param app: The app that is polling the queue.
        :param worker: The worker that is polling the queue.
        """
        poll_logger = PrefixAdapter(logger, {"prefix": f"Q.{self.name}"})
        poll_logger.info(
            f"Queue {self.name!r} is now active and polling for new jobs."
        )

        while True:
            async with app.pool.connection() as conn:
                self._wake_up.clear()

                # If we wouldn't be able to run a job even if we had one, we
                # should just wait. Pre-fetching can be advantageous, but
                # IMO it causes more headache (as seen with Celery and future
                # scheduled tasks) than it's worth.
                maximum_jobs_to_poll = self.concurrency - len(self.executor)
                if maximum_jobs_to_poll <= 0:
                    poll_logger.debug(
                        "No capacity for new jobs, skipping poll."
                    )
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
                    fetched=len(jobs),
                    worker_id=worker.worker_id,
                    queue=self.name,
                )

                for job in jobs:
                    poll_logger.debug(
                        f"Found job {job.id}, pushing to executor."
                    )
                    await self.executor.push(job)

            done, pending = await asyncio.wait(
                [
                    asyncio.create_task(asyncio.sleep(self.polling_interval)),
                    asyncio.create_task(self._wake_up.wait()),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in pending:
                task.cancel()

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
                                created_at ASC,
                                id ASC
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

            return [
                JobInstance(
                    id=record["id"],
                    func=record["payload"]["func"],
                    kwargs=record["payload"]["kwargs"],
                    priority=record["priority"],
                    scheduled_at=record["scheduled_at"],
                    created_at=record["created_at"],
                    started_at=record["started_at"],
                    completed_at=record["completed_at"],
                    attempts=record["attempts"],
                    max_attempts=record["max_attempts"],
                    state=record["state"],
                    limits=[
                        Limit.deserialize(limit)
                        for limit in record["payload"]["limits"]
                    ],
                )
                for record in records
            ]

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

    async def push_jobs(
        self, conn: AsyncConnection, jobs: list[Job], prefix: str = "chancy_"
    ):
        """
        Push one or more jobs onto the queue.

        :param conn: The database connection to use.
        :param jobs: The jobs to push onto the queue.
        :param prefix: The prefix to use for the database tables.
        """
        jobs_table = sql.Identifier(f"{prefix}jobs")

        async with conn.cursor() as cursor:
            async with conn.transaction():
                await cursor.executemany(
                    sql.SQL(
                        """
                        INSERT INTO
                            {jobs} (
                                queue,
                                payload,
                                priority,
                                max_attempts,
                                scheduled_at
                        ) VALUES (%s, %s, %s, %s, %s);
                        """
                    ).format(jobs=jobs_table),
                    [
                        (
                            self.name,
                            Json(
                                {
                                    "func": job.func,
                                    "kwargs": job.kwargs or {},
                                    "limits": [
                                        limit.serialize()
                                        for limit in job.limits
                                    ],
                                }
                            ),
                            job.priority,
                            job.max_attempts,
                            job.scheduled_at,
                        )
                        for job in jobs
                    ],
                )
                await cursor.execute(
                    sql.SQL("SELECT pg_notify({events}, {event});").format(
                        events=sql.Literal(f"{prefix}events"),
                        event=sql.Literal(
                            json.dumps({"t": "pushed", "q": self.name})
                        ),
                    )
                )

    async def wake_up(self):
        """
        Wake up the queue.

        If the queue's `start()` coroutine is running and sleeping waiting for
        the next poll, this method will wake it up and cause it to poll
        immediately.
        """
        self._wake_up.set()
