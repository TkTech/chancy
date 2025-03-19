import dataclasses
import datetime
import enum
from dataclasses import KW_ONLY


@dataclasses.dataclass(frozen=True)
class Queue:
    """
    Queues are used to group jobs together and determine how they should be
    processed. Each queue has a name, concurrency level, and a set of tags that
    determine which workers can process jobs from the queue.

    Queue's must be declared using :func:`~chancy.app.Chancy.declare` before workers
    will be able to process jobs from them.

    .. code-block:: python

        async with Chancy("postgresql://localhost/postgres") as chancy:
            await chancy.declare(Queue(name="default", concurrency=4))

    By default, this queue will shortly be picked up by all running workers and
    begin processing jobs. If you want to instead apply it to specific workers,
    you can assign it using "tags":

    .. code-block:: python

        async with Chancy("postgresql://localhost/postgres") as chancy:
            await chancy.declare(Queue(name="default", concurrency=4, tags={"reporting"}))

    This will only be picked up by workers that have the "reporting" tag:

    .. code-block:: python

        async with Chancy("postgresql://localhost/postgres") as chancy:
            async with Worker(chancy, tags={"reporting"}) as worker:
                await worker.wait_for_shutdown()

    Queues can have a global rate limit applied to them, which will be enforced
    across all workers processing jobs from the queue:

    .. code-block:: python

        async with Chancy("postgresql://localhost/postgres") as chancy:
            await chancy.declare(
                Queue(name="default", rate_limit=10, rate_limit_window=60)
            )

    This will limit the queue to processing 10 jobs per minute across all
    workers. If the rate limit is exceeded, jobs will be skipped until the
    rate limit window has passed. Combined with the AsyncExecutor, this can be
    a very easy way to work with external APIs.

    .. note::

        Rate limiting is done with a fixed window algorithm for simplicity.
        If you need to do something custom, subclass the worker and
        re-implement :func:`~chancy.worker.Worker.fetch_jobs`.
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
    #: If None, the concurrency level will be determined by the worker's
    #: core count, unless overridden by a plugin.
    concurrency: int | None = None
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
    polling_interval: int = 5
    #: An optional global rate limit to apply to this queue. All workers
    #: processing jobs from this queue will be subject to this limit.
    rate_limit: int | None = None
    #: The period of time over which the rate limit applies (in seconds).
    rate_limit_window: int | None = None
    #: If set, the time at which the queue should automatically reset to the
    #: active state. This can be used to implement a "pause for X seconds"
    #: feature for circuit breakers and such.
    resume_at: datetime.datetime | None = None

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
            resume_at=data.get("resume_at"),
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
            "resume_at": self.resume_at,
        }
