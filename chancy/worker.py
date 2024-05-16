import json
import asyncio
import uuid
from asyncio import TaskGroup

from psycopg import sql
from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from chancy.app import Chancy, Queue
from chancy.logger import logger, PrefixAdapter
from chancy.migrate import Migrator


class Worker:
    """
    A Chancy job worker.

    Any number of workers may be started to process jobs from the queue, on
    any number of machines. The workers will automatically detect and process
    jobs as they are added to the queue.

    One worker will be elected as the leader, and will be responsible for
    running plugins that are marked as leader-only, such as database maintenance
    tasks.
    """

    def __init__(self, app: Chancy, worker_id: str = None):
        self.app = app
        self.pool = AsyncConnectionPool(
            self.app.dsn,
            open=False,
            check=AsyncConnectionPool.check_connection,
            min_size=2,
            max_size=3,
        )
        self.worker_id = worker_id or str(uuid.uuid4())

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
            self.app.dsn,
            "chancy",
            "chancy.migrations",
            prefix=self.app.prefix,
        )

        async with self.pool.connection() as conn:
            if await mig.is_migration_required(conn):
                core_logger.error(
                    f"The database is out of date and requires a migration"
                    f" before the worker can start."
                )
                return

        async with TaskGroup() as tg:
            tg.create_task(self.poll_available_tasks())
            if not self.app.disable_events:
                tg.create_task(self.listen_for_events())

    async def poll_available_tasks(self):
        """
        Periodically pulls tasks from the queue.
        """
        poll_logger = PrefixAdapter(logger, {"prefix": "POLLER"})

        poll_logger.info("Started periodic polling for tasks.")

        async with self.pool.connection() as conn:
            while True:
                poll_logger.debug("Polling for tasks...")

                for queue in self.app.queues:
                    jobs = await self.fetch_available_jobs(conn, queue)

                poll_logger.debug("Completed a polling cycle.")
                await asyncio.sleep(self.app.poller_delay)

    async def fetch_available_jobs(self, conn: AsyncConnection, queue: Queue):
        """
        Fetches available jobs from the queue.

        Pulls jobs from the queue that are ready to be processed, locking the
        jobs to prevent other workers from processing them.
        """
        jobs_table = sql.Identifier(f"{self.app.prefix}jobs")

        async with conn.cursor(row_factory=dict_row) as cur:
            async with conn.transaction():
                await cur.execute(
                    sql.SQL(
                        """
                        SELECT
                            id
                        FROM
                            {jobs}
                        WHERE
                            queue = {queue}
                        AND
                            state = 'pending'
                        AND
                            attempts < max_attempts
                        ORDER BY
                            priority ASC,
                            created_at ASC,
                            id ASC
                        FOR UPDATE SKIP LOCKED
                        """
                    ).format(jobs=jobs_table, queue=queue.name),
                )

                await cur.execute(
                    sql.SQL(
                        """
                        UPDATE
                            {jobs}
                        SET
                            started_at = NOW(),
                            attempts = attempts + 1,
                            state = 'running'
                        """
                    ).format(jobs=jobs_table)
                )

    async def listen_for_events(self):
        """
        Listens for events in the cluster using the Postgres LISTEN/NOTIFY
        mechanism.
        """
        event_logger = PrefixAdapter(logger, {"prefix": "EVENTS"})

        event_logger.info("Starting event listener...")
        async with self.pool.connection() as conn:
            await conn.execute(
                sql.SQL("LISTEN {}").format(
                    sql.Identifier(f"{self.app.prefix}events")
                )
            )

            event_logger.info("Now listening for events.")
            async for notice in conn.notifies():
                try:
                    j = json.loads(notice.payload)
                except json.JSONDecodeError:
                    event_logger.error(
                        f"Received invalid JSON payload: {notice.payload}"
                    )
                    continue

                event_logger.debug(f"Received event: {j}")

    async def __aenter__(self):
        await self.pool.open()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.pool.close()
        return False
