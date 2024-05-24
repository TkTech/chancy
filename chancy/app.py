import re
import enum
import dataclasses
from functools import cached_property

from psycopg_pool import AsyncConnectionPool

from chancy.migrate import Migrator


@dataclasses.dataclass(frozen=True)
class Queue:
    """
    Defines a named queue for processing jobs in Chancy.
    """

    class State(enum.IntEnum):
        """
        The state of a queue.
        """

        #: The queue is currently processing jobs.
        ACTIVE = 1
        #: The queue is paused and not processing jobs.
        PAUSED = 2

    #: A name for the queue, must only be a-z and underscore.
    name: str
    #: Maximum number of jobs to process concurrently (per-worker).
    concurrency: int = 1
    #: The state of the queue.
    state: State = State.ACTIVE


@dataclasses.dataclass
class Chancy:
    """
    The main application object for Chancy.
    """

    #: A valid engine connection string.
    dsn: str
    #: A list of queues that this app should be aware of for processing jobs.
    queues: list[Queue] = dataclasses.field(default_factory=list)
    #: A list of plugins to enable for this application.
    plugins: list[str] = dataclasses.field(default_factory=list)
    #: The prefix to use for all Chancy tables in the database.
    prefix: str = "chancy_"
    #: The minimum number of seconds to wait when polling the queue.
    poller_delay: int = 5
    #: Disable events, relying on polling only.
    disable_events: bool = False
    #: The maximum number of jobs a worker can process before restarting.
    maximum_jobs_per_worker: int = 100

    #: The minimum number of connections to keep in the pool.
    min_pool_size: int = 2
    #: The maximum number of connections to keep in the pool.
    max_pool_size: int = 3

    def __post_init__(self):
        # Ensure all defined queues have distinct names.
        queue_names = [queue.name for queue in self.queues]
        if len(queue_names) != len(set(queue_names)):
            raise ValueError("Duplicate queue names are not allowed.")

        # Ensure all queue names are valid a-zA-Z and underscore.
        for queue_name in queue_names:
            if not re.match(r"^[a-z_]+$", queue_name):
                raise ValueError("Queue names must only contain a-z, and _.")

    async def migrate(self):
        """
        Migrate the database to the latest schema version.

        This method will apply any pending migrations to the database to bring
        it up to the latest schema version. If the database is already up-to-
        date, this method will do nothing.
        """
        migrator = Migrator("chancy", "chancy.migrations", prefix=self.prefix)
        async with self.pool.connection() as conn:
            await migrator.migrate(conn)

    @cached_property
    def pool(self):
        """
        A connection pool for the Chancy application.
        """
        return AsyncConnectionPool(
            self.dsn,
            open=False,
            check=AsyncConnectionPool.check_connection,
            min_size=self.min_pool_size,
            max_size=self.max_pool_size,
        )

    async def __aenter__(self):
        await self.pool.open()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.pool.close()
        return False
