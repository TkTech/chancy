import abc
import typing
import traceback
import dataclasses
from datetime import datetime, timezone

from chancy.job import JobInstance
from chancy.queue import Queue

if typing.TYPE_CHECKING:
    from chancy.worker import Worker


class Executor(abc.ABC):
    """
    The executor is responsible for managing the execution of jobs in a job
    pool.
    """

    def __init__(self, worker: "Worker", queue: "Queue"):
        self.worker = worker
        self.queue = queue

    @abc.abstractmethod
    async def push(self, job: JobInstance):
        """
        Push a job onto the job pool.
        """

    async def job_completed(
        self, job: JobInstance, exc: Exception | None = None
    ):
        """
        Called when a job has completed.

        This method should be called by the executor when a job has completed
        execution. It will update the job's state in the queue and handle
        retries if necessary.

        :param job: The job that has completed.
        :param exc: The exception that was raised during execution, if any.
        """
        if exc is not None:
            new_state = (
                JobInstance.State.FAILED
                if job.attempts + 1 >= job.max_attempts
                else JobInstance.State.RETRYING
            )

            new_instance = dataclasses.replace(
                job,
                state=new_state,
                attempts=job.attempts + 1,
                errors=[
                    *job.errors,
                    {
                        "traceback": traceback.format_exception(
                            type(exc), exc, exc.__traceback__
                        ),
                        "attempt": job.attempts,
                    },
                ],
            )
        else:
            now = datetime.now(tz=timezone.utc)
            new_instance = dataclasses.replace(
                job,
                state=JobInstance.State.SUCCEEDED,
                completed_at=now,
            )

        await self.worker.push_update(new_instance)

    @abc.abstractmethod
    def __len__(self):
        """
        Get the number of pending and running jobs in the pool.
        """
