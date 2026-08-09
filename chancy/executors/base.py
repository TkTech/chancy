import abc
import asyncio
import dataclasses
import enum
import inspect
import traceback
import typing
from abc import ABC
from asyncio import Future
from collections.abc import Callable
from datetime import UTC, datetime
from functools import cached_property
from typing import Any

import chancy.queue
from chancy.job import Limit, QueuedJob, Reference

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

    class Capability(enum.IntFlag):
        """Features supported by an executor."""

        NONE = 0
        SYNC_JOBS = enum.auto()
        ASYNC_JOBS = enum.auto()
        CANCELLATION = enum.auto()
        AUTOMATIC_TIME_LIMITS = enum.auto()
        COOPERATIVE_TIME_LIMITS = enum.auto()
        MEMORY_LIMITS = enum.auto()

    capabilities = Capability.NONE

    def __init__(self, worker: "Worker", queue: chancy.queue.Queue):
        self.worker = worker
        self.queue = queue

    @classmethod
    def get_capabilities(cls) -> Capability:
        """Return the features supported by this executor."""
        return cls.capabilities

    @classmethod
    def supports(cls, capability: Capability) -> bool:
        """Return whether this executor supports all given capabilities."""
        return (cls.get_capabilities() & capability) == capability

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
            now = datetime.now(tz=UTC)
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
                completed_at=(datetime.now(tz=UTC) if is_failure else None),
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
    def _resolve_function_and_kwargs(
        job: QueuedJob,
    ) -> tuple[Callable, dict, bool]:
        """
        Finds the function which should be executed for the given job and
        returns its keyword arguments and whether it accepts job context.

        :param job: The job instance to get the function and arguments for.
        :return: The function, keyword arguments, and context support.
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
        kwargs = dict(job.kwargs or {})
        has_job_context = False
        for param_name, param in sig.parameters.items():
            if param.kind != param.KEYWORD_ONLY:
                continue

            if not param.annotation:
                continue

            try:
                if issubclass(param.annotation, QueuedJob):
                    kwargs[param_name] = job
                    has_job_context = True
            except TypeError:
                continue

        return func, kwargs, has_job_context

    @staticmethod
    def get_function_and_kwargs(job: QueuedJob) -> tuple[Callable, dict]:
        """
        Find the function which should be executed for the given job and
        return its keyword arguments.

        :param job: The job instance to get the function and arguments for.
        :return: A tuple containing the function and its keyword arguments.
        """
        func, kwargs, _ = Executor._resolve_function_and_kwargs(job)

        return func, kwargs

    @staticmethod
    def get_limit(job: QueuedJob, limit_type: Limit.Type) -> int | None:
        """Return the first configured limit of a given type."""
        return next(
            (limit.value for limit in job.limits if limit.type_ == limit_type),
            None,
        )

    @classmethod
    def prepare_job_for_execution(
        cls, job: QueuedJob
    ) -> tuple[QueuedJob, Callable, dict]:
        """Validate and prepare a job according to executor capabilities."""
        capabilities = cls.get_capabilities()
        time_limit = cls.get_limit(job, Limit.Type.TIME)

        if cls.get_limit(job, Limit.Type.MEMORY) is not None and not (
            capabilities & cls.Capability.MEMORY_LIMITS
        ):
            raise ValueError(f"{cls.__name__} does not support memory limits.")

        cooperative_time_limit = False
        if time_limit is not None and not (
            capabilities & cls.Capability.AUTOMATIC_TIME_LIMITS
        ):
            if not (capabilities & cls.Capability.COOPERATIVE_TIME_LIMITS):
                raise ValueError(
                    f"{cls.__name__} does not support time limits."
                )
            job = job._with_time_limit(time_limit)
            cooperative_time_limit = True

        func, kwargs, has_job_context = cls._resolve_function_and_kwargs(job)
        function_capability = (
            cls.Capability.ASYNC_JOBS
            if inspect.iscoroutinefunction(func)
            else cls.Capability.SYNC_JOBS
        )
        if not (capabilities & function_capability):
            function_type = (
                "asynchronous"
                if function_capability == cls.Capability.ASYNC_JOBS
                else "synchronous"
            )
            raise ValueError(
                f"{cls.__name__} does not support {function_type} jobs."
            )

        if cooperative_time_limit and not has_job_context:
            raise ValueError(
                "Jobs using cooperative time limits must accept a"
                " keyword-only QueuedJob context and call"
                " context.checkpoint()."
            )

        return job, func, kwargs

    @staticmethod
    def run_function(func: Callable, kwargs: dict) -> Any:
        """Run a synchronous or asynchronous function to completion."""
        if not inspect.iscoroutinefunction(func):
            return func(**kwargs)

        return asyncio.run(func(**kwargs))

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

    def get_running_job(self, ref: Reference) -> QueuedJob | None:
        """
        Get a running job by its reference.

        :param ref: The reference to the job to find.
        :return: The job if found, None otherwise.
        """
        for job in self.get_running_jobs():
            if job.id == ref.identifier:
                return job
        return None

    @abc.abstractmethod
    def get_running_jobs(self) -> list[QueuedJob]:
        """
        Get all jobs currently running in this executor.

        :return: A list of running jobs.
        """

    def is_job_running(self, ref: Reference) -> bool:
        """
        Check if a job is currently running in this executor.

        :param ref: The reference to the job to check.
        :return: True if the job is running, False otherwise.
        """
        return self.get_running_job(ref) is not None

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

    def __init__(self, worker: "Worker", queue: chancy.queue.Queue):
        super().__init__(worker, queue)
        self.jobs: dict[Future, QueuedJob] = {}

    async def cancel(self, ref: Reference):
        for future, job in self.jobs.items():
            if job.id == ref.identifier:
                future.cancel()
                return

    def get_running_jobs(self) -> list[QueuedJob]:
        return list(self.jobs.values())

    def __len__(self):
        return len(self.jobs)

    @classmethod
    def job_wrapper(cls, job: QueuedJob) -> tuple[QueuedJob, Any]:
        """Run a job with cooperative limit handling."""
        job, func, kwargs = cls.prepare_job_for_execution(job)
        result = cls.run_function(func, kwargs)
        job.checkpoint()
        return job, result
