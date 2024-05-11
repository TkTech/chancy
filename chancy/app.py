import dataclasses


@dataclasses.dataclass
class Queue:
    """
    A Queue object defines a single named queue.
    """

    #: A name for the queue, must only be a-z and underscore.
    name: str
    #: Maximum number of jobs to process concurrently.
    concurrency: int = 1


@dataclasses.dataclass
class Chancy:
    """
    A Chancy application provides the interface for defining the Chancy queues,
    and various other configuration.
    """

    #: A valid Postgres connection string.
    postgres: str
    #: A list of queues that this app should be aware of for processing jobs.
    queues: list[Queue]
    #: A list of plugins to enable for this application.
    plugins: list[str] = dataclasses.field(default_factory=list)

    #: The prefix to use for all Chancy tables in the database.
    prefix: str = "chancy_"
    #: The minimum number of seconds to wait when polling the queue.
    poller_delay: int = 5

    #: The schema version that this app is compatible with. Override at your
    #: own risk.
    allowed_schema_versions: list[int] = dataclasses.field(
        default_factory=lambda: [1]
    )


app = Chancy("", queues=[Queue("default")])
