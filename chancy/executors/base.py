import abc
import inspect
import typing
import traceback
import dataclasses
from abc import ABC
from asyncio import Future
from datetime import datetime, timezone
from functools import cached_property
from typing import Callable, Any

from chancy.job import QueuedJob, Reference
from chancy.queue import Queue

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
    async def push(self, job: QueuedJob):
        """
        Push a job onto the job pool.
        """

    async def on_job_starting(self, job: QueuedJob) -> QueuedJob:
        """
        Called when a job has been retrieved from the queue and is about to
        start.
        """
        for plugin in self.worker.chancy.plugins.values():
            try:
                job = await plugin.on_job_starting(job=job, worker=self.worker)
            except NotImplementedError:
                continue

        return job

    async def on_job_completed(
        self,
        *,
        job: QueuedJob,
        exc: Exception | None = None,
        result: Any = None,
    ):
        """
        Called when a job has completed.

        This method should be called by the executor when a job has completed
        execution. It will update the job's state in the queue and handle
        retries if necessary.

        :param job: The job that has completed.
        :param exc: The exception that was raised during execution, if any.
        :param result: The result of the job, if any.
        """
        if exc is None:
            now = datetime.now(tz=timezone.utc)
            new_instance = dataclasses.replace(
                job,
                state=QueuedJob.State.SUCCEEDED,
                completed_at=now,
                attempts=job.attempts + 1,
            )
        else:
            is_failure = job.attempts + 1 >= job.max_attempts

            new_state = (
                QueuedJob.State.FAILED
                if is_failure
                else QueuedJob.State.RETRYING
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

            self.worker.chancy.log.debug(
                f"Job {job.id} ({job.func}({job.kwargs!r}) failed with an"
                f" exception",
                exc_info=(type(exc), exc, exc.__traceback__),
            )

        # Each plugin has a chance to modify the job instance after it's
        # completed.
        for plugin in self.worker.chancy.plugins.values():
            try:
                new_instance = await plugin.on_job_completed(
                    job=new_instance,
                    worker=self.worker,
                    exc=exc,
                    result=result,
                )
            except NotImplementedError:
                continue

        await self.worker.queue_update(new_instance)
        await self.worker.on_job_completed(queue=self.queue, job=new_instance)

    @staticmethod
    def get_function_and_kwargs(job: QueuedJob) -> tuple[Callable, dict]:
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
        sig = inspect.signature(func)
        kwargs = job.kwargs or {}
        for param_name, param in sig.parameters.items():
            if not param.kind == param.KEYWORD_ONLY:
                continue

            if not param.annotation:
                continue

            try:
                if issubclass(param.annotation, QueuedJob):
                    kwargs[param_name] = job
            except TypeError:
                continue

        return func, kwargs

    @abc.abstractmethod
    async def stop(self):
        """
        Stop the executor, giving it a chance to clean up any resources it
        may have allocated to running jobs.

        It is not safe to use the executor after this method has been
        called.
        """

    @abc.abstractmethod
    async def cancel(self, ref: Reference):
        """
        Attempt to cancel a running job.

        It's not guaranteed that the job will be cancelled, as it may have
        already completed by the time this method is called or the executor
        may not support cancellation.

        :param ref: The reference to the job to cancel.
        """

    @abc.abstractmethod
    def get_default_concurrency(self) -> int:
        """
        Get the default concurrency level for this executor.

        This method is called when the queue's concurrency level is set to
        None. It should return the number of jobs that can be processed
        concurrently by this executor.
        """

    @cached_property
    def concurrency(self) -> int:
        if self.queue.concurrency is None:
            return self.get_default_concurrency()
        return self.queue.concurrency

    @property
    def free_slots(self) -> int:
        return self.concurrency - len(self)

    @abc.abstractmethod
    def __len__(self):
        """
        Get the number of jobs currently within the executor.
        """

    def __repr__(self):
        return (
            f"<{self.__class__.__name__}"
            f" worker={self.worker!r}"
            f" queue={self.queue.name!r}>"
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.stop()


class ConcurrentExecutor(Executor, ABC):
    """
    Base class for executors based off of the concurrent.futures.Executor
    class with common functionality.
    """

    def __init__(self, worker: "Worker", queue: "Queue"):
        super().__init__(worker, queue)
        self.jobs: dict[Future, QueuedJob] = {}

    async def cancel(self, ref: Reference):
        for future, job in self.jobs.items():
            if job.id == ref.identifier:
                future.cancel()
                return

    def __len__(self):
        return len(self.jobs)
