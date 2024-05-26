"""
The :class:`chancy.worker.Worker` class is the main entry point for running
workers in a Chancy application.
"""

import inspect
import os
import json
import uuid
import signal
import asyncio
import resource
import threading
import dataclasses
from asyncio import TaskGroup
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, Future
from datetime import datetime, timezone
from functools import partial
from typing import Optional

from psycopg import sql
from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg.errors import LockNotAvailable

from chancy.app import Chancy, Queue, Job, Limit, JobContext
from chancy.logger import logger, PrefixAdapter
from chancy.migrate import Migrator


class _TimeoutThread(threading.Thread):
    """
    A thread that will raise a TimeoutError after a specified number of
    seconds.
    """

    def __init__(self, timeout: int, cancel: threading.Event):
        super().__init__()
        self.timeout = timeout
        self.cancel = cancel

    def run(self):
        if self.cancel.wait(self.timeout) is True:
            # Our call to wait() returning True means that the flag was set
            # before the timeout elapsed, so we should cancel our alarm.
            return

        # If we reach this point, the timeout has elapsed, and we should raise
        # a TimeoutError back in the main process thread.
        os.kill(os.getpid(), signal.SIGALRM)


def _job_signal_handler(signum, frame):
    """
    A signal handler for the job wrapper process.
    """
    if signum == signal.SIGALRM:
        raise TimeoutError("Job timed out.")


def _job_wrapper(context: JobContext):
    """
    Wraps the job execution in a function that can be run in a separate
    process.

    .. note::

        It cannot be overstated - while this function implements timeouts and
        memory limits, it does so in the understanding that the job function is
        not a bad actor. It's possible for a poorly-behaved job to ignore the
        timeout and memory limits, or to otherwise interfere with the operation
        of the worker.
    """
    cleanup = []

    for limit in context.job.limits:
        match limit.type:
            case Limit.Type.MEMORY:
                previous_soft, _ = resource.getrlimit(resource.RLIMIT_AS)
                resource.setrlimit(
                    resource.RLIMIT_AS,
                    (limit.value, -1),
                )
                # Ensure we reset the memory limit after the job has completed,
                # since our process will be reused by a task that may not have
                # an explicit memory limit.
                cleanup.append(
                    lambda: resource.setrlimit(
                        resource.RLIMIT_AS,
                        (previous_soft, -1),
                    )
                )
            case Limit.Type.TIME:
                # Use a thread to raise a signal after the timeout has elapsed.
                # This emulates SIGALRM on platforms which don't support it,
                # like NT, and allows us to cancel the alarm if the job
                # completes before the timeout.
                signal.signal(signal.SIGALRM, _job_signal_handler)
                cancel = threading.Event()
                timeout_thread = _TimeoutThread(limit.value, cancel)
                timeout_thread.start()
                cleanup.append(lambda: cancel.set())

    # Each job stores the function that should be called as a string in the
    # format "module.path.to.function".
    mod_name, func_name = context.job.func.rsplit(".", 1)
    mod = __import__(mod_name, fromlist=[func_name])
    func = getattr(mod, func_name)

    kwargs = context.job.kwargs or {}

    sig = inspect.signature(func)
    if "job_context" in sig.parameters:
        kwargs["job_context"] = context

    try:
        func(**kwargs)
    finally:
        for clean in cleanup:
            clean()


@dataclasses.dataclass(frozen=True)
class StateChange:
    """
    A change to the state of a job.

    Used to aggregate changes to the state of a job before applying them in a
    single transaction the next time a worker polls for jobs from a queue.
    """

    context: JobContext
    state: str
    completed_at: Optional[datetime] = None


class Nanny:
    """
    A shim around a ProcessPoolExecutor.

    This class is responsible for submitting jobs to the pool and associating
    them with their futures for later retrieval.
    """

    def __init__(self, app: Chancy, queue: Queue):
        self.app = app
        self.processes: dict[Future, JobContext] = {}
        self.pool = ProcessPoolExecutor(
            max_workers=queue.concurrency,
            max_tasks_per_child=queue.maximum_jobs_per_worker,
            initializer=queue.on_setup,
        )

    def submit(self, context: JobContext) -> Future:
        """
        Submit a job to the pool for execution, returning the future of the
        task.
        """
        future: Future = self.pool.submit(_job_wrapper, context=context)
        self.processes[future] = context
        return future

    def pop(self, future: Future) -> JobContext:
        """
        Retrieve a job from the pool by its future, removing it from the pool.

        Raises a KeyError if the future is not found.
        """
        return self.processes.pop(future)

    def __len__(self):
        return len(self.processes)

    def __iter__(self):
        return iter(self.processes)

    def __getitem__(self, key):
        return self.processes[key]


class Worker:
    """
    A Chancy worker.

    This class is responsible for polling the queue for available jobs,
    submitting them to the process pool for execution, and handling the
    results.

    Each worker should have a globally unique worker ID, which is used to
    acquire the leader lock in the cluster and track statistics. If no worker
    ID is provided, a random UUID will be generated.

    This worker uses a process pool to execute jobs. While this has higher
    overhead than threading or asyncio, it provides a high degree of isolation
    between jobs, and allows for the enforcement of memory and time limits on
    individual jobs while completely sidestepping the GIL.

    Example:

    .. code-block:: python

        async def main():
            async with Chancy(
                dsn="postgresql://username:password@localhost:8190/postgres",
                queues=[
                    Queue(name="default", concurrency=1),
                ],
            ) as app:
                await Worker(app).start()

    :param app: The Chancy application to run the worker for.
    :param worker_id: An optional, globally-unique worker ID to use for this
                      worker.
    """

    def __init__(self, app: Chancy, *, worker_id: str = None):
        self.app = app
        self.worker_id = worker_id or str(uuid.uuid4())
        self.pools: dict[Queue, Nanny] = {
            queue: Nanny(app, queue) for queue in app.queues
        }
        self.pending_changes: dict[Queue, asyncio.Queue] = defaultdict(
            asyncio.Queue
        )

    async def start(self):
        """
        Start the worker.

        The worker will immediately begin pulling jobs from the queue, and
        make itself available as a possible leader in the cluster.

        Each worker requires two connections to Postgres; one for polling for
        jobs, and one for listening for events.

        In some cases, workers running plugins may require additional
        connections.
        """
        core_logger = PrefixAdapter(logger, {"prefix": "CORE"})

        mig = Migrator(
            "chancy",
            "chancy.migrations",
            prefix=self.app.prefix,
        )

        async with self.app.pool.connection() as conn:
            if await mig.is_migration_required(conn):
                core_logger.error(
                    f"The database is out of date and requires a migration"
                    f" before the worker can start."
                )
                return

        async with TaskGroup() as tg:
            tg.create_task(self._poll_event_loop())
            tg.create_task(self._leadership_loop())
            if not self.app.disable_events:
                tg.create_task(self._listen_for_events())

    async def _poll_event_loop(self):
        poll_logger = PrefixAdapter(logger, {"prefix": "POLL"})
        poll_logger.info("Started periodic polling for tasks.")

        while True:
            async with self.app.pool.connection() as conn:
                for queue in self.app.queues:
                    if queue.state == Queue.State.PAUSED:
                        poll_logger.debug(
                            f"Queue {queue.name} is paused, skipping."
                        )
                        continue

                    jobs = await self._fetch_available_jobs(conn, queue)
                    poll_logger.debug(
                        f"Found {len(jobs)} job(s) in queue {queue.name}."
                    )
                    for job in jobs:
                        future = self.pools[queue].submit(job)
                        future.add_done_callback(
                            partial(self._handle_job_result, queue=queue)
                        )

                await asyncio.sleep(self.app.job_poller_interval)

    async def _fetch_available_jobs(
        self, conn: AsyncConnection, queue: Queue
    ) -> list[JobContext]:
        """
        Fetches available jobs from the queue.

        Pulls jobs from the queue that are ready to be processed, locking the
        jobs to prevent other workers from processing them.

        If there are pending changes to the job state, they will be applied
        before fetching new jobs.
        """
        jobs_table = sql.Identifier(f"{self.app.prefix}jobs")

        maximum_jobs_to_fetch = queue.concurrency - len(self.pools[queue])

        # If we have no jobs to fetch and no pending changes, we can skip
        # the query stage entirely and just NOP out.
        if not maximum_jobs_to_fetch and self.pending_changes[queue].empty():
            return []

        async with conn.cursor(row_factory=dict_row) as cur:
            async with conn.transaction():
                # To reduce the number of transactions we need to make, we'll
                # batch up any pending changes to the job state and apply them
                # in a single transaction.
                if not self.pending_changes[queue].empty():
                    all_pending_changes = []
                    while not self.pending_changes[queue].empty():
                        all_pending_changes.append(
                            await self.pending_changes[queue].get()
                        )

                    await cur.executemany(
                        sql.SQL(
                            """
                            UPDATE {jobs}
                            SET
                                state = %s,
                                completed_at = %s
                            WHERE
                                id = %s
                            """
                        ).format(jobs=jobs_table),
                        [
                            (
                                change.state,
                                change.completed_at,
                                change.context.id,
                            )
                            for change in all_pending_changes
                        ],
                    )

                if maximum_jobs_to_fetch <= 0:
                    return []

                await cur.execute(
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
                                state = 'pending'
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
                            state = 'running'
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
                        "maximum_jobs_to_fetch": maximum_jobs_to_fetch,
                    },
                )
                return [
                    JobContext(
                        job=Job(
                            func=row["payload"]["func"],
                            kwargs=row["payload"]["kwargs"],
                            limits=[
                                Limit.deserialize(limit)
                                for limit in row["payload"]["limits"]
                            ],
                            scheduled_at=row["scheduled_at"],
                            max_attempts=row["max_attempts"],
                            priority=row["priority"],
                        ),
                        attempts=row["attempts"],
                        id=row["id"],
                        created_at=row["created_at"],
                        started_at=row["started_at"],
                        completed_at=row["completed_at"],
                        state=row["state"],
                    )
                    for row in await cur.fetchall()
                ]

    async def _listen_for_events(self):
        """
        Listens for events in the cluster using the Postgres LISTEN/NOTIFY
        mechanism.
        """
        event_logger = PrefixAdapter(logger, {"prefix": "BUS"})

        async with self.app.pool.connection() as conn:
            await conn.execute(
                sql.SQL("LISTEN {}").format(
                    sql.Identifier(f"{self.app.prefix}events")
                )
            )

            event_logger.info("Now listening for cluster events.")
            async for notice in conn.notifies():
                try:
                    j = json.loads(notice.payload)
                except json.JSONDecodeError:
                    event_logger.error(
                        f"Received invalid JSON payload: {notice.payload}"
                    )
                    continue

                event_logger.debug(f"Received event: {j}")

    def _handle_job_result(self, future: Future, queue: Queue):
        """
        Handles the result of a job execution.
        """
        job_logger = PrefixAdapter(logger, {"prefix": "JOB"})

        context = self.pools[queue].pop(future)
        job = context.job

        # Check to see if something went wrong with the job.
        exc = future.exception()
        if exc is not None:
            # If the job has a maximum number of attempts, and we haven't
            # exceeded that number, we'll put the job back into the pending
            # state.
            if job.max_attempts and context.attempts < job.max_attempts:
                self.pending_changes[queue].put_nowait(
                    StateChange(
                        context=context,
                        state="pending",
                    )
                )
                job_logger.exception(
                    f"Job {context.id} failed with an exception"
                    f" ({type(exc)}), retry attempt {context.attempts}/"
                    f"{job.max_attempts}.",
                    exc_info=exc,
                )
                return

            self.pending_changes[queue].put_nowait(
                StateChange(
                    context=context,
                    state="failed",
                    completed_at=datetime.now(tz=timezone.utc),
                )
            )
            job_logger.error(
                f"Job {context.id} failed with an exception and has no free"
                f" retry attempts, marking as failed."
            )
            return

        self.pending_changes[queue].put_nowait(
            StateChange(
                context=context,
                state="completed",
                completed_at=datetime.now(tz=timezone.utc),
            )
        )
        job_logger.info(f"Job {context.id!r} completed successfully.")

    async def _leadership_loop(self):
        """
        A loop that periodically attempts to acquire the leader lock.

        The leader lock is used to ensure that certain plugins are only run by
        a single worker at a time.
        """
        leader_logger = PrefixAdapter(logger, {"prefix": "LEADER"})

        while True:
            async with self.app.pool.connection() as conn:
                is_leader = await self._try_acquire_leader(conn)
                if is_leader:
                    leader_logger.info(
                        f"Acquired/refreshed leader lock for worker"
                        f" {self.worker_id}."
                    )
                else:
                    leader_logger.debug(
                        f"Failed to acquire leader lock for worker"
                        f" {self.worker_id}."
                    )

                await asyncio.sleep(self.app.leadership_timeout)

    async def _try_acquire_leader(self, conn: AsyncConnection) -> bool:
        """
        Attempt to acquire the leader lock for this worker.

        If the leader lock is already held by another worker, this method will
        return False. Otherwise, it will return True.

        It's possible for there to be no current valid leader while waiting on
        a worker to time out, but there should never be > 1 leader.
        """
        now = datetime.now(tz=timezone.utc)
        leaders = sql.Identifier(f"{self.app.prefix}leader")

        async with conn.cursor() as cur:
            try:
                # It's important that this SELECT covers the entire table to
                # apply the FOR UPDATE lock.
                await cur.execute(
                    sql.SQL(
                        """
                        SELECT
                            worker_id,
                            last_seen
                        FROM {leaders} FOR UPDATE
                        """
                    ).format(leaders=leaders)
                )
                result = await cur.fetchone()
                if result:
                    worker_id, last_seen = result
                    since = (now - last_seen).total_seconds()
                    if (
                        since > self.app.leadership_timeout
                        or worker_id == self.worker_id
                    ):
                        # Either we own the lock already, and we just need to
                        # update the heartbeat, or the lock is stale, we can
                        # take it.
                        await cur.execute(
                            sql.SQL(
                                """
                                UPDATE
                                    {leaders}
                                SET
                                    worker_id = %(worker_id)s,
                                    last_seen = %(last_seen)s
                                """
                            ).format(leaders=leaders),
                            {"worker_id": self.worker_id, "last_seen": now},
                        )
                        await conn.commit()
                        return True
                    else:
                        await conn.rollback()
                        return False
                else:
                    await cur.execute(
                        sql.SQL(
                            """
                            INSERT INTO
                                {leaders}
                                (worker_id, last_seen)
                            VALUES
                                (%(worker_id)s, %(last_seen)s)
                            ON CONFLICT (worker_id) DO UPDATE SET
                                last_seen = %(last_seen)s
                            """
                        ).format(leaders=leaders),
                        {"worker_id": self.worker_id, "last_seen": now},
                    )
                    await conn.commit()
                    return True
            except LockNotAvailable:
                await conn.rollback()
                return False
