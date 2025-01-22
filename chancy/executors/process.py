import asyncio
import functools
import multiprocessing
import os
import sys
from multiprocessing.context import BaseContext

try:
    import resource
except ImportError:
    # Only available on Unix-y systems (AKA not Windows)
    resource = None

import signal
from asyncio import Future, CancelledError
from concurrent.futures import ProcessPoolExecutor
from typing import Callable, Any

from chancy import Reference
from chancy.executors.base import ConcurrentExecutor
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
        if sys.platform != "win32":
            signal.signal(signal.SIGALRM, cls.job_signal_handler)
            signal.signal(signal.SIGUSR1, cls.job_signal_handler)

    async def push(self, job: QueuedJob) -> Future:
        job = await self.on_job_starting(job)

        future: Future = self.pool.submit(
            self.job_wrapper, job, self.pids_for_job
        )
        future.add_done_callback(
            functools.partial(
                self._on_job_completed, loop=asyncio.get_running_loop()
            )
        )
        self.jobs[future] = job
        time_limit = next(
            (
                limit.value
                for limit in job.limits
                if limit.type_ == Limit.Type.TIME
            ),
            None,
        )
        if time_limit is not None:
            self.timeouts[job.id] = asyncio.create_task(
                self._handle_timeout(job.id, time_limit)
            )

        return future

    async def _handle_timeout(self, job_id: str, time_limit: int):
        try:
            await asyncio.sleep(time_limit)
            pid = self.pids_for_job.get(job_id)
            if pid is not None:
                os.kill(pid, signal.SIGALRM)
        except asyncio.CancelledError:
            pass

    @classmethod
    def job_wrapper(cls, job: QueuedJob, pids_for_job) -> tuple[QueuedJob, Any]:
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
            func, kwargs = cls.get_function_and_kwargs(job)
            if asyncio.iscoroutinefunction(func):
                raise ValueError(
                    f"Function {job.func!r} is an async function, which is not"
                    f" supported by the {cls.__name__!r} executor. Use the "
                    "AsyncExecutor instead."
                )

            for limit in job.limits:
                match limit.type_:
                    case Limit.Type.MEMORY:
                        if resource is None:
                            raise ValueError(
                                "Memory limits are not supported on this"
                                " platform due to the resource module not being"
                                " available."
                            )

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

            result = func(**kwargs)
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
        if signum == signal.SIGALRM:
            raise TimeoutError("Job timed out.")
        if signum == signal.SIGUSR1:
            raise CancelledError("Job was cancelled.")

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

        f = asyncio.run_coroutine_threadsafe(
            self.on_job_completed(job=job, exc=exc, result=result),
            loop,
        )
        f.result()

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
        await super().cancel(ref)
        pid = self.pids_for_job.get(ref.identifier)
        if pid is not None:
            os.kill(pid, signal.SIGUSR1)
