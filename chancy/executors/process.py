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
from asyncio import CancelledError, Future
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from typing import Any
from uuid import UUID

from chancy.executors.base import ConcurrentExecutor, Executor
from chancy.job import Limit, QueuedJob


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
    # Child-local state; cleared before returning control to the process pool.
    _current_execution: tuple[UUID, Any] | None = None

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
        # Execution-specific cancellation requests. SIGUSR1 only wakes the
        # child; this marker determines whether its current job is cancelled.
        self.pending_cancellations = self.manager.dict()
        self.timeouts: dict[UUID, asyncio.Task] = {}
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
        ``maximum_jobs_per_worker`` is reached (if
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
        self.jobs[future] = job
        future.add_done_callback(
            functools.partial(
                self._on_job_completed, loop=asyncio.get_running_loop()
            )
        )
        time_limit = self.get_limit(job, Limit.Type.TIME)
        if time_limit is not None and self.supports(
            Executor.Capability.AUTOMATIC_TIME_LIMITS
        ):
            execution_id = job.claim_id or job.id
            self.timeouts[execution_id] = asyncio.create_task(
                self._handle_timeout(execution_id, time_limit)
            )

        return future

    async def _handle_timeout(self, execution_id: UUID, time_limit: int):
        try:
            await asyncio.sleep(time_limit)
            pid = self.pids_for_job.get(execution_id)
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
        execution_id = job.claim_id or job.id
        try:
            pids_for_job[execution_id] = os.getpid()
            cls._current_execution = (execution_id, pending_cancellations)
            # Also honor requests received before PID registration.
            if hasattr(signal, "SIGUSR1"):
                cls.job_signal_handler(signal.SIGUSR1, None)
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
                            lambda previous_soft=previous_soft: (
                                resource.setrlimit(
                                    resource.RLIMIT_AS, (previous_soft, -1)
                                )
                            )
                        )

            result = cls.run_function(func, kwargs)
        finally:
            cls._current_execution = None
            pids_for_job.pop(execution_id)
            pending_cancellations.pop(execution_id, None)
            for clean in cleanup:
                clean()

        return job, result

    @classmethod
    def job_signal_handler(cls, signum: int, frame):
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
            if cls._current_execution is None:
                return
            execution_id, pending_cancellations = cls._current_execution
            # A second signal must not interrupt the manager proxy's RPC.
            previous_mask = signal.pthread_sigmask(
                signal.SIG_BLOCK, {signal.SIGUSR1}
            )
            try:
                if pending_cancellations.pop(execution_id, None) is not None:
                    cls._current_execution = None
                    raise CancelledError("Job was cancelled.")
            finally:
                signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
        if getattr(signal, "SIGALRM", None) == signum:
            raise TimeoutError("Job timeout out.")

    def _on_job_completed(
        self, future: Future, loop: asyncio.AbstractEventLoop
    ):
        job = self.jobs.get(future)
        if job is None:
            return

        timeout_task = self.timeouts.pop(job.claim_id or job.id, None)
        if timeout_task is not None:
            timeout_task.cancel()

        super()._on_job_completed(future, loop)

    def _shutdown_blocking(self):
        super()._shutdown_blocking()
        self.manager.shutdown()

    async def stop(self):
        for task in self.timeouts.values():
            task.cancel()

        await super().stop()

    async def _stop_on_cancel(self):
        for task in self.timeouts.values():
            task.cancel()

        if hasattr(signal, "SIGUSR1"):
            for execution_id, pid in list(self.pids_for_job.items()):
                self.pending_cancellations[execution_id] = True
                try:
                    os.kill(pid, signal.SIGUSR1)
                except (ProcessLookupError, PermissionError):
                    pass

        await super()._stop_on_cancel()
        await asyncio.to_thread(self.manager.shutdown)

    async def cancel_execution(self, job: QueuedJob):
        """
        Make an attempt to cancel a running job.

        It's not guaranteed that the job will be cancelled, nor is it
        guaranteed that the job will be cancelled in a timely manner. For
        example if the job is running a long computation in a C extension,
        it may not be possible to interrupt it until it returns.

        :param job: The specific execution to cancel.
        """
        future = next(
            (
                f
                for f, j in self.jobs.items()
                if j.id == job.id and j.claim_id == job.claim_id
            ),
            None,
        )
        if future is None or future.done():
            return

        if future.cancel() or not self.supports(
            Executor.Capability.CANCELLATION
        ):
            return

        execution_id = job.claim_id or job.id
        self.pending_cancellations[execution_id] = True
        pid = self.pids_for_job.get(execution_id)
        if pid is not None:
            try:
                os.kill(pid, signal.SIGUSR1)
            except OSError:
                # The process may have exited or become inaccessible.
                pass

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
