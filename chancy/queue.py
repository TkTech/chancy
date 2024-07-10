import abc
import asyncio
from typing import Callable, TYPE_CHECKING
from functools import cached_property

from chancy.executor import Executor
from chancy.executors.process import ProcessExecutor
from chancy.job import Reference, JobInstance, Job
from chancy.plugin import Plugin, PluginScope

if TYPE_CHECKING:
    from chancy.app import Chancy


class QueuePlugin(Plugin, abc.ABC):
    """
    A specialized plugin that provides a queue for pull jobs from, and clients
    to push jobs to.

    This class is an abstract base class that should be subclassed to implement
    a queue provider, such as a Redis-backed queue or an SQS-backed queue.

    :param name: The name of the queue.
    :param concurrency: The maximum number of jobs that can be run concurrently
                        by each worker on this queue.
    :param executor: The executor to use for running jobs.
    :param polling_interval: The interval at which to poll the queue for new
                             jobs.
    """

    def __init__(
        self,
        name: str,
        *,
        concurrency: int = 1,
        executor: Callable[["QueuePlugin"], Executor] | None = None,
        polling_interval: int | None = 5,
    ):
        super().__init__()
        self.name = name
        self.concurrency = concurrency
        self.polling_interval = polling_interval
        self._executor = ProcessExecutor if executor is None else executor
        # A queue of pending updates to jobs that need to be applied on the
        # next fetch.
        self.pending_updates = asyncio.Queue()

    @cached_property
    def executor(self) -> Executor:
        return self._executor(self)

    @classmethod
    def get_scope(cls) -> PluginScope:
        return PluginScope.QUEUE

    @abc.abstractmethod
    async def push(self, app, jobs: list[Job]):
        """
        Push one or more jobs onto the queue.

        :param app: The Chancy application.
        :param jobs: The jobs to push onto the queue.
        """

    @abc.abstractmethod
    async def get_job(self, app: "Chancy", ref: Reference) -> JobInstance:
        """
        Get a job instance by reference.
        """
