import abc
import inspect
import typing
import traceback
import dataclasses
from datetime import datetime, timezone
from typing import get_type_hints, Callable

from chancy.job import JobInstance
from chancy.queue import Queue
from chancy.retry import Retry, handle_retry, MaxRetriesExceededError

if typing.TYPE_CHECKING:
    from chancy.worker import Worker


class Executor(abc.ABC):
    """
    The base class for all executors.

    Executors are responsible for managing the execution of jobs after they've
    been retrieved from a queue.

    See :class:`~chancy.executors.process.ProcessExecutor` and
    :class:`~chancy.executors.asyncex.AsyncExecutor` for examples of built-in
    executors.
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
        if exc is None:
            now = datetime.now(tz=timezone.utc)
            new_instance = dataclasses.replace(
                job,
                state=JobInstance.State.SUCCEEDED,
                completed_at=now,
                attempts=job.attempts + 1,
            )
        elif isinstance(exc, Retry):
            try:
                new_instance = handle_retry(job, exc)
            except MaxRetriesExceededError:
                # We've exceeded the maximum number of retries, so mark the
                # job as failed.
                new_instance = dataclasses.replace(
                    job,
                    state=JobInstance.State.FAILED,
                    completed_at=datetime.now(tz=timezone.utc),
                    attempts=job.attempts + 1,
                    errors=[
                        *job.errors,
                        {
                            "traceback": "".join(
                                traceback.format_exception(
                                    type(exc), exc, exc.__traceback__
                                )
                            ),
                            "attempt": job.attempts,
                        },
                    ],
                )
        else:
            is_failure = job.attempts + 1 >= job.max_attempts

            new_state = (
                JobInstance.State.FAILED
                if is_failure
                else JobInstance.State.RETRYING
            )

            new_instance = dataclasses.replace(
                job,
                state=new_state,
                attempts=job.attempts + 1,
                completed_at=(
                    datetime.now(tz=timezone.utc) if is_failure else None
                ),
                errors=[
                    *job.errors,
                    {
                        "traceback": "".join(
                            traceback.format_exception(
                                type(exc), exc, exc.__traceback__
                            )
                        ),
                        "attempt": job.attempts,
                    },
                ],
            )

        # Each plugin has a chance to modify the job instance after it's
        # completed.
        for plugin in self.worker.chancy.plugins:
            try:
                new_instance = await plugin.on_job_completed(
                    new_instance, worker=self.worker, exc=exc
                )
            except NotImplementedError:
                continue

        await self.worker.queue_update(new_instance)

    @staticmethod
    def get_function_and_kwargs(job: JobInstance) -> tuple[Callable, dict]:
        """
        Finds the function which should be executed for the given job and
        returns its keyword arguments.

        :param job: The job instance to get the function and arguments for.
        :return: A tuple containing the function and its keyword arguments.
        """
        mod_name, func_name = job.func.rsplit(".", 1)
        mod = __import__(mod_name, fromlist=[func_name])
        try:
            func = getattr(mod, func_name)
        except AttributeError:
            raise AttributeError(
                f"Could not find function {func_name} in module {mod_name}."
            )

        # We take a look at the type signature for the function to see if the
        # user has specified that the job instance should be passed as a
        # keyword argument.
        type_hints = get_type_hints(func)
        sig = inspect.signature(func)
        kwargs = job.kwargs or {}
        for param_name, param in sig.parameters.items():
            if (
                param.kind == param.KEYWORD_ONLY
                and type_hints.get(param_name) is JobInstance
            ):
                kwargs[param_name] = job

        return func, kwargs

    @abc.abstractmethod
    def __len__(self):
        """
        Get the number of pending and running jobs in the pool.
        """
