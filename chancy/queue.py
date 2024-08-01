import dataclasses


@dataclasses.dataclass(frozen=True)
class Queue:
    class State:
        #: The queue is active and jobs can be processed.
        ACTIVE = "active"
        #: The queue is paused and no jobs will be processed.
        PAUSED = "paused"

    #: A globally unique identifier for the queue.
    name: str
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
