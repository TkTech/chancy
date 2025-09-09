import os
import threading
from concurrent.futures import ThreadPoolExecutor, Future
import asyncio
import functools
from typing import Any

from chancy.executors.base import ConcurrentExecutor
from chancy.job import QueuedJob, Limit


class ThreadedExecutor(ConcurrentExecutor):
    """
    An Executor which uses a thread pool to run its jobs.

    This executor is useful for running I/O-bound jobs concurrently without the
    overhead of separate processes. It's not suitable for CPU-bound tasks due
    to Python's Global SubInterpreter Lock (GIL).

    When working with existing asyncio code, it's often easier and more
    efficient to use the :class:`~chancy.executors.asyncex.AsyncExecutor`
    instead, as it can run a very large number of jobs concurrently.

    To use this executor, simply pass the import path to this class in the
    ``executor`` field of your queue configuration or use the
    :class:`~chancy.app.Chancy.Executor` shortcut:

    .. code-block:: python

        async with Chancy("postgresql://localhost/postgres") as chancy:
            await chancy.declare(
                Queue(
                    name="default",
                    executor=Chancy.Executor.Threaded,
                )
            )

    :param worker: The worker instance associated with this executor.
    :param queue: The queue that this executor is associated with.
    """

    def __init__(self, worker, queue):
        super().__init__(worker, queue)
        self.pool = ThreadPoolExecutor(max_workers=queue.concurrency)

    async def push(self, job: QueuedJob) -> Future:
        job = await self.on_job_starting(job)
        future: Future = self.pool.submit(self.job_wrapper, job)
        self.jobs[future] = job
        future.add_done_callback(
            functools.partial(
                self._on_job_completed, loop=asyncio.get_running_loop()
            )
        )
        return future

    def job_wrapper(self, job: QueuedJob) -> tuple[QueuedJob, Any]:
        """
        This is the function that is actually started by the thread pool
        executor. It's responsible for setting up necessary limits,
        running the job, and returning the result.
        """
        func, kwargs = self.get_function_and_kwargs(job)

        time_limit = next(
            (
                limit.value
                for limit in job.limits
                if limit.type_ == Limit.Type.TIME
            ),
            None,
        )

        timer = None
        if time_limit:
            timer = threading.Timer(time_limit, self._timeout_handler)
            timer.start()

        try:
            if asyncio.iscoroutinefunction(func):
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    result = loop.run_until_complete(func(**kwargs))
                finally:
                    loop.run_until_complete(loop.shutdown_asyncgens())
                    loop.close()
            else:
                result = func(**kwargs)
        finally:
            if timer:
                timer.cancel()

        return job, result

    @staticmethod
    def _timeout_handler():
        raise TimeoutError("Job timed out.")

    def _on_job_completed(
        self, future: Future, loop: asyncio.AbstractEventLoop
    ):
        job = self.jobs.pop(future)

        result = None
        exc = future.exception()
        if exc is None:
            job, result = future.result()

        asyncio.run_coroutine_threadsafe(
            self.on_job_completed(job=job, exc=exc, result=result),
            loop,
        )

    async def stop(self):
        self.pool.shutdown(cancel_futures=True)
        await super().stop()

    def get_default_concurrency(self) -> int:
        """
        Get the default concurrency level for this executor.

        This method is called when the queue's concurrency level is set to
        None. It should return the number of jobs that can be processed
        concurrently by this executor.

        On Python 3.13+, defaults to the number of logical CPUs on the system
        plus 4. On older versions of Python, defaults to the number of CPUs on
        the system plus 4. This mimics the behavior of Python's built-in
        ThreadPoolExecutor.
        """
        # Only available in 3.13+
        if hasattr(os, "process_cpu_count"):
            return min(32, (os.process_cpu_count() or 1) + 4)
        return min(32, (os.cpu_count() or 1) + 4)
