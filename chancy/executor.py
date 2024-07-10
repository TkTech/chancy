import abc
import typing

from chancy.job import JobInstance

if typing.TYPE_CHECKING:
    from chancy.queue import QueuePlugin


class Executor(abc.ABC):
    """
    The executor is responsible for managing the execution of jobs in a job
    pool.
    """

    def __init__(self, queue: "QueuePlugin"):
        self.queue = queue

    @abc.abstractmethod
    async def push(self, job: JobInstance):
        """
        Push a job onto the job pool.
        """

    @abc.abstractmethod
    def __len__(self):
        """
        Get the number of pending and running jobs in the pool.
        """
