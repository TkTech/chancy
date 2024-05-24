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

from chancy.app import Chancy, Queue, Job, Limit
from chancy.logger import logger, PrefixAdapter
from chancy.migrate import Migrator
from chancy.utils import timed_block


class TimeoutThread(threading.Thread):
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


def _job_wrapper(job: Job):
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

    for limit in job.limits:
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
                signal.signal(signal.SIGALRM, _job_signal_handler)
                cancel = threading.Event()
                timeout_thread = TimeoutThread(limit.value, cancel)
                timeout_thread.start()
                cleanup.append(lambda: cancel.set())

    # Each job stores the function that should be called as a string in the
    # format "module.path.to.function".
    mod_name, func_name = job.func.rsplit(".", 1)
    mod = __import__(mod_name, fromlist=[func_name])
    func = getattr(mod, func_name)

    try:
        func(**job.kwargs or {})
    finally:
        for clean in cleanup:
            clean()


@dataclasses.dataclass(frozen=True)
class StateChange:
    """
    A change to the state of a job.
    """

    job: Job
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
        self.processes = {}
        self.pool = ProcessPoolExecutor(
            max_workers=queue.concurrency,
            max_tasks_per_child=app.maximum_jobs_per_worker,
        )

    def submit(self, job: Job) -> Future:
        """
        Submit a job to the pool for execution, returning the future of the
        task.
        """
        future: Future = self.pool.submit(_job_wrapper, job=job)
        self.processes[future] = job
        return future

    def pop(self, future: Future) -> Job:
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

    The worker will also listen for events in the cluster, such as job
    enqueueing and leader retirement.

    :param app: The Chancy application to run the worker for.
    :param worker_id: An optional worker ID to use for this worker.
    """

    def __init__(self, app: Chancy, worker_id: str = None):
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
            tg.create_task(self._poll_available_tasks())
            if not self.app.disable_events:
                tg.create_task(self._listen_for_events())

    async def _poll_available_tasks(self):
        """
        Periodically pulls tasks from the queue.
        """
        poll_logger = PrefixAdapter(logger, {"prefix": "POLL"})
        poll_logger.info("Started periodic polling for tasks.")

        async with self.app.pool.connection() as conn:
            while True:
                with timed_block() as timer:
                    for queue in self.app.queues:
                        jobs = await self._fetch_available_jobs(conn, queue)
                        poll_logger.debug(
                            f"Found {len(jobs)} job(s) in queue {queue.name}."
                        )
                        for job in jobs:
                            future = self.pools[queue].submit(job)
                            future.add_done_callback(
                                partial(self._handle_job_result, queue=queue)
                            )

                    poll_logger.debug(
                        f"Completed a polling cycle in {timer.elapsed:.4f}s."
                    )

                await asyncio.sleep(self.app.poller_delay)

    async def _fetch_available_jobs(
        self, conn: AsyncConnection, queue: Queue
    ) -> list[Job]:
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
                                change.job.id,
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
                return [Job.deserialize(row) for row in await cur.fetchall()]

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

        job = self.pools[queue].pop(future)

        # Check to see if something went wrong with the job.
        exc = future.exception()
        if exc is not None:
            # If the job has a maximum number of attempts, and we haven't
            # exceeded that number, we'll put the job back into the pending
            # state.
            if job.max_attempts and job.attempts < job.max_attempts:
                self.pending_changes[queue].put_nowait(
                    StateChange(
                        job=job,
                        state="pending",
                    )
                )
                job_logger.exception(
                    f"Job {job.id} failed with an exception ({type(exc)}), "
                    f" retry attempt {job.attempts}/{job.max_attempts}.",
                    exc_info=exc,
                )
                return

            self.pending_changes[queue].put_nowait(
                StateChange(
                    job=job,
                    state="failed",
                    completed_at=datetime.now(tz=timezone.utc),
                )
            )
            job_logger.error(
                f"Job {job.id} failed with an exception and has no free retry"
                f" attempts, marking as failed."
            )
            return

        self.pending_changes[queue].put_nowait(
            StateChange(
                job=job,
                state="completed",
                completed_at=datetime.now(tz=timezone.utc),
            )
        )
        job_logger.info(f"Job {job.id!r} completed successfully.")
