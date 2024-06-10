import asyncio

from psycopg import sql, AsyncConnection
from psycopg.rows import dict_row
from psycopg.types.json import Json

from chancy.executor import Executor, JobInstance, Limit, Job
from chancy.executors.process import ProcessExecutor
from chancy.logger import PrefixAdapter, logger


class Queue:
    """
    A named Chancy queue.

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
        concurrency: int,
        executor: Executor | None = None,
        polling_interval: int | None = 5,
    ):
        self.name = name
        self.concurrency = concurrency
        self.executor = executor or ProcessExecutor(self)
        self.polling_interval = polling_interval
        self.pending_updates = asyncio.Queue()

    async def poll(self, app, *, worker_id: str):
        """
        Continuously polls the queue for new jobs.

        This method will run indefinitely, polling the queue for new jobs and
        running them as they become available.

        If you want to pull jobs from the queue without further processing,
        you can use the `fetch_jobs` method directly instead.

        :param app: The app that is polling the queue.
        :param worker_id: The ID of the worker polling the queue.
        """
        poll_logger = PrefixAdapter(logger, {"prefix": f"Q.{self.name}"})
        poll_logger.info(
            f"Queue {self.name!r} is now active and polling for new jobs."
        )

        while True:
            async with app.pool.connection() as conn:
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
                    worker_id=worker_id,
                )

                for job in jobs:
                    poll_logger.debug(
                        f"Found job {job.id}, pushing to executor."
                    )
                    await self.executor.push(job)

            poll_logger.debug(
                f"Polling finished, idling for {self.polling_interval}s."
            )
            await asyncio.sleep(self.polling_interval)

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
                                    limit.serialize() for limit in job.limits
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
