import dataclasses
import enum
from dataclasses import KW_ONLY


@dataclasses.dataclass(frozen=True)
class Queue:
    """
    Represents a queue in a Chancy cluster.

    Queues are used to group jobs together and determine how they should be
    processed. Each queue has a name, concurrency level, and a set of tags that
    determine which workers can process jobs from the queue.

    Queue's must be declared using :func:`~chancy.app.Chancy.declare` before workers
    will be able to process jobs from them.
    """

    class State(enum.Enum):
        #: The queue is active and jobs can be processed.
        ACTIVE = "active"
        #: The queue is paused and no jobs will be processed.
        PAUSED = "paused"

    #: A globally unique identifier for the queue.
    name: str

    _ = KW_ONLY
    #: The number of jobs that can be processed concurrently per worker.
    concurrency: int = 1
    #: The tags that determine which workers will process this queue.
    tags: set[str] = dataclasses.field(default_factory=lambda: {r".*"})
    #: The state of the queue.
    state: State = State.ACTIVE
    #: The import path to the executor that should be used to process jobs in
    #: this queue.
    executor: str = "chancy.executors.process.ProcessExecutor"
    #: The options to pass to the executor's constructor.
    executor_options: dict = dataclasses.field(default_factory=dict)
    #: The number of seconds to wait between polling the queue for new jobs.
    polling_interval: int = 1
    #: An optional global rate limit to apply to this queue. All workers
    #: processing jobs from this queue will be subject to this limit.
    rate_limit: int | None = None
    #: The period of time over which the rate limit applies (in seconds).
    rate_limit_window: int | None = None

    @classmethod
    def unpack(cls, data: dict) -> "Queue":
        """
        Unpack a serialized queue object into a Queue instance.
        """
        return cls(
            name=data["name"],
            concurrency=data["concurrency"],
            tags=set(data["tags"]),
            state=cls.State(data["state"]),
            executor=data["executor"],
            executor_options=data["executor_options"],
            polling_interval=data["polling_interval"],
            rate_limit=data.get("rate_limit"),
            rate_limit_window=data.get("rate_limit_window"),
        )

    def pack(self) -> dict:
        """
        Pack the queue into a dictionary that can be serialized and used to
        recreate the queue later.
        """
        return {
            "name": self.name,
            "concurrency": self.concurrency,
            "tags": list(self.tags),
            "state": self.state.value,
            "executor": self.executor,
            "executor_options": self.executor_options,
            "polling_interval": self.polling_interval,
            "rate_limit": self.rate_limit,
            "rate_limit_window": self.rate_limit_window,
        }
