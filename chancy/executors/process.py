import asyncio
import functools
import multiprocessing
import os
from multiprocessing.context import BaseContext


try:
    import resource
except ImportError:
    # Windows doesn't have the `resource` module
    resource = None
import signal
from asyncio import Future, CancelledError
from concurrent.futures import ProcessPoolExecutor
from typing import Callable, Any
from uuid import UUID

from chancy import Reference
from chancy.executors.base import ConcurrentExecutor, Executor
from chancy.job import QueuedJob, Limit


class ProcessExecutor(ConcurrentExecutor):
    """
    An Executor which uses a process pool to run its jobs.

    This executor is useful for running jobs that are CPU-bound, avoiding the
    GIL (Global SubInterpreter Lock) that Python uses to ensure thread safety.

    To use this executor, simply pass the import path to this class in the
    ``executor`` field of your queue configuration or use the
    :class:`~chancy.app.Chancy.Executor` shortcut:

    .. code-block:: python

        async with Chancy("postgresql://localhost/postgres") as chancy:
            await chancy.declare(
                Queue(
                    name="default",
                    concurrency=10,
                    executor=Chancy.Executor.Process,
                    executor_options={
                        "maximum_jobs_per_worker": 100,
                    }
                )
            )


    :param queue: The queue that this executor is associated with.
    :param maximum_jobs_per_worker: The maximum number of jobs that each worker
                                    can run before being replaced. Handy if you
                                    are stuck using a library with memory leaks.
    :param mp_context: The multiprocessing context to use. If not provided, the
                       default "spawn" context will be used, which is the
                       safest option on all platforms.
    """

    capabilities = (
        Executor.Capability.SYNC_JOBS | Executor.Capability.ASYNC_JOBS
    )

    @classmethod
    def get_capabilities(cls) -> Executor.Capability:
        capabilities = cls.capabilities
        if hasattr(signal, "SIGUSR1"):
            capabilities |= Executor.Capability.CANCELLATION
        if hasattr(signal, "SIGALRM"):
            capabilities |= Executor.Capability.AUTOMATIC_TIME_LIMITS
        if resource is not None and hasattr(resource, "RLIMIT_AS"):
            capabilities |= Executor.Capability.MEMORY_LIMITS
        return capabilities

    def __init__(
        self,
        worker,
        queue,
        *,
        maximum_jobs_per_worker: int = 100,
        mp_context: BaseContext | None = None,
    ):
        super().__init__(worker, queue)
        # Becomes the default on Linux as of 3.14 due to potential crashes
        # in the `fork` method if the child process also works with threads.
        # We're using `spawn` explicitly to get ahead of the curve, however
        # this is slower than `fork`.
        ctx = mp_context or multiprocessing.get_context("spawn")

        self.manager = ctx.Manager()
        self.pids_for_job = self.manager.dict()
        # Jobs whose cancellation arrived before the child process registered
        # its PID. Consumed at the top of ``job_wrapper`` to close the race
        # between dispatch and SIGUSR1 delivery.
        self.pending_cancellations = self.manager.dict()
        self.timeouts: dict[str, asyncio.Task] = {}
        self.pool = ProcessPoolExecutor(
            max_workers=queue.concurrency,
            max_tasks_per_child=maximum_jobs_per_worker,
            initializer=self.on_initialize_worker,
            mp_context=ctx,
        )

    @classmethod
    def on_initialize_worker(cls):
        """
        This method is called in each worker process before it begins running
        jobs. It can be used to perform any necessary setup, such as loading
        NLTK datasets or calling ``django.setup()``.

        This isn't called once per job but once per worker process until
        :attr:`~ProcessExecutor.maximum_jobs_per_worker` is reached (if
        set). After that, the worker process is replaced with a new one.

        .. note::

            Care should be taken when overriding this method, as it is called
            within a separate process and may not have access to the same
            resources as the main process.
        """
        if hasattr(signal, "SIGALRM"):
            signal.signal(signal.SIGALRM, cls.job_signal_handler)
        if hasattr(signal, "SIGUSR1"):
            signal.signal(signal.SIGUSR1, cls.job_signal_handler)

        if os.environ.get("DJANGO_SETTINGS_MODULE"):
            try:
                import django
            except ImportError:
                return

            django.setup()

    async def push(self, job: QueuedJob) -> Future:
        job = await self.on_job_starting(job)

        future: Future = self.pool.submit(
            self.job_wrapper,
            job,
            self.pids_for_job,
            self.pending_cancellations,
        )
        future.add_done_callback(
            functools.partial(
                self._on_job_completed, loop=asyncio.get_running_loop()
            )
        )
        self.jobs[future] = job
        time_limit = self.get_limit(job, Limit.Type.TIME)
        if time_limit is not None and self.supports(
            Executor.Capability.AUTOMATIC_TIME_LIMITS
        ):
            self.timeouts[job.id] = asyncio.create_task(
                self._handle_timeout(job.id, time_limit)
            )

        return future

    async def _handle_timeout(self, job_id: UUID, time_limit: int):
        try:
            await asyncio.sleep(time_limit)
            pid = self.pids_for_job.get(job_id)
            if pid is not None:
                os.kill(pid, signal.SIGALRM)
        except asyncio.CancelledError:
            pass

    @classmethod
    def job_wrapper(
        cls, job: QueuedJob, pids_for_job, pending_cancellations
    ) -> tuple[QueuedJob, Any]:
        """
        This is the function that is actually started by the process pool
        executor. It's responsible for setting up necessary signals and limits,
        running the job, and returning the result.

        Subclasses can override this method to provide additional functionality
        or to change the way that jobs are run.

        .. note::

            Care should be taken when overriding this method, as it is called
            within a separate process and may not have access to the same
            resources as the main process.
        """
        cleanup: list[Callable] = []
        try:
            pids_for_job[job.id] = os.getpid()
            # Cancel arrived during child startup, before we could register
            # to receive SIGUSR1. Honor it now.
            if pending_cancellations.pop(job.id, None) is not None:
                raise CancelledError("Job was cancelled.")
            job, func, kwargs = cls.prepare_job_for_execution(job)

            for limit in job.limits:
                match limit.type_:
                    case Limit.Type.MEMORY:
                        previous_soft, _ = resource.getrlimit(
                            resource.RLIMIT_AS
                        )
                        resource.setrlimit(
                            resource.RLIMIT_AS, (limit.value, -1)
                        )
                        cleanup.append(
                            lambda: resource.setrlimit(
                                resource.RLIMIT_AS, (previous_soft, -1)
                            )
                        )

            result = cls.run_function(func, kwargs)
        finally:
            pids_for_job.pop(job.id)
            for clean in cleanup:
                clean()

        return job, result

    @staticmethod
    def job_signal_handler(signum: int, frame):
        """
        Handles signals sent to a running job process.

        Subclasses can override this method to provide additional functionality
        or to change the way that signals are handled.

        .. note::

            Care should be taken when overriding this method, as it is called
            within a separate process and may not have access to the same
            resources as the main process.
        """
        if getattr(signal, "SIGUSR1", None) == signum:
            raise CancelledError("Job was cancelled.")
        if getattr(signal, "SIGALRM", None) == signum:
            raise TimeoutError("Job timeout out.")

    def _on_job_completed(
        self, future: Future, loop: asyncio.AbstractEventLoop
    ):
        job = self.jobs.pop(future)

        timeout_task = self.timeouts.pop(job.id, None)
        if timeout_task is not None:
            timeout_task.cancel()

        result = None
        exc = future.exception()
        if exc is None:
            job, result = future.result()

        asyncio.run_coroutine_threadsafe(
            self.on_job_completed(job=job, exc=exc, result=result),
            loop,
        )

    async def stop(self):
        for task in self.timeouts.values():
            task.cancel()

        self.pool.shutdown(cancel_futures=True)
        self.manager.shutdown()

    async def cancel(self, ref: Reference):
        """
        Make an attempt to cancel a running job.

        It's not guaranteed that the job will be cancelled, nor is it
        guaranteed that the job will be cancelled in a timely manner. For
        example if the job is running a long computation in a C extension,
        it may not be possible to interrupt it until it returns.

        :param ref: The reference to the job to cancel.
        """
        future = next(
            (f for f, j in self.jobs.items() if j.id == ref.identifier),
            None,
        )
        if future is None:
            return

        future.cancel()
        if not self.supports(Executor.Capability.CANCELLATION):
            return

        pid = self.pids_for_job.get(ref.identifier)
        if pid is not None:
            os.kill(pid, signal.SIGUSR1)
        else:
            # Child hasn't registered its PID yet. ``job_wrapper`` will
            # consume this marker as its first action after registering.
            self.pending_cancellations[ref.identifier] = True

    def get_default_concurrency(self) -> int:
        """
        Get the default concurrency level for this executor.

        This method is called when the queue's concurrency level is set to
        None. It should return the number of jobs that can be processed
        concurrently by this executor.

        Default is the number of CPUs on the system.
        """
        # Only available in 3.13+
        if hasattr(os, "process_cpu_count"):
            return os.process_cpu_count() or 1
        return os.cpu_count() or 1
