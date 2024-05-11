import json
import asyncio
from asyncio import TaskGroup

from psycopg import AsyncConnection
from psycopg import sql

from chancy.app import Chancy
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

    def __init__(self, app: Chancy):
        self.app = app

    async def start(self):
        """
        Start the worker.

        The worker will immediately begin pulling jobs from the queue, and
        make itself available as a possible leader in the cluster.
        """
        core_logger = PrefixAdapter(logger, {"prefix": "CORE"})

        mig = Migrator(self.app)

        conn = await AsyncConnection.connect(self.app.postgres, autocommit=True)
        if await mig.is_migration_required(conn):
            core_logger.error(
                f"The database is out of date and requires a migration before"
                f" the worker can start."
            )
            return

        async with TaskGroup() as tg:
            tg.create_task(self.poll_available_tasks())
            tg.create_task(self.listen_for_events())

    async def poll_available_tasks(self):
        """
        Periodically pulls tasks from the queue.
        """
        poll_logger = PrefixAdapter(logger, {"prefix": "POLLER"})

        poll_logger.info("Started periodic polling for tasks.")
        while True:
            poll_logger.debug("Completed a polling cycle.")
            await asyncio.sleep(self.app.poller_delay)

    async def listen_for_events(self):
        """
        Listens for events in the cluster using the Postgres LISTEN/NOTIFY
        mechanism.
        """
        event_logger = PrefixAdapter(logger, {"prefix": "EVENTS"})

        event_logger.info("Starting event listener...")
        conn = await AsyncConnection.connect(self.app.postgres, autocommit=True)
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
