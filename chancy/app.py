import re
import enum
import dataclasses

from chancy.migrate import Migrator


@dataclasses.dataclass
class Queue:
    """
    A Queue object defines a single named queue.
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
    #: Maximum number of jobs to process concurrently.
    concurrency: int = 1
    #: The state of the queue.
    state: State = State.ACTIVE


@dataclasses.dataclass
class Chancy:
    """
    A Chancy application provides the interface for defining the Chancy queues,
    and various other configuration.
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
        """
        migrator = Migrator(
            self.dsn, "chancy", "chancy.migrations", prefix=self.prefix
        )
        async with migrator:
            await migrator.migrate()
