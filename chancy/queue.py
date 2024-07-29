import dataclasses


@dataclasses.dataclass(frozen=True)
class Queue:
    class State:
        ACTIVE = "active"
        PAUSED = "paused"

    #: The name of the queue.
    name: str
    #: The number of jobs that can be processed concurrently per worker.
    concurrency: int = 1
    #: The tags that determine which workers will process this queue.
    tags: set[str] = dataclasses.field(default_factory=set)
    #: The state of the queue.
    state: State = State.ACTIVE
    #: The name of the executor to use.
    executor: str = "chancy.executors.process.ProcessExecutor"
    #: The options to pass to the executor.
    executor_options: dict = dataclasses.field(default_factory=dict)
    #: The number of seconds to wait between polling the queue for new jobs.
    polling_interval: int = 1
