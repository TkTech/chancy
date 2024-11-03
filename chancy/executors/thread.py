import threading
from concurrent.futures import ThreadPoolExecutor, Future
import asyncio
from typing import Dict
import functools

from chancy.executors.base import Executor
from chancy.job import QueuedJob, Limit


class ThreadedExecutor(Executor):
    """
    An Executor which uses a thread pool to run its jobs.

    This executor is useful for running I/O-bound jobs concurrently without the
    overhead of separate processes. It's not suitable for CPU-bound tasks due
    to Python's Global Interpreter Lock (GIL).

    When working with existing asyncio code, it's often easier and more
    efficient to use the :class:`~chancy.executors.asyncex.AsyncExecutor`
    instead, as it can run a very large number of jobs concurrently.

    To use this executor, simply pass the import path to this class in the
    ``executor`` field of your queue configuration:

    .. code-block:: python

        async with Chancy(dsn="postgresql://localhost/postgres") as chancy:
            await chancy.declare(
                Queue(
                    name="default",
                    executor="chancy.executors.thread.ThreadedExecutor",
                )
            )

    :param worker: The worker instance associated with this executor.
    :param queue: The queue that this executor is associated with.
    """

    def __init__(self, worker, queue):
        super().__init__(worker, queue)
        self.pool = ThreadPoolExecutor(max_workers=queue.concurrency)
        self.jobs: Dict[Future, QueuedJob] = {}

    async def push(self, job: QueuedJob) -> Future:
        future: Future = self.pool.submit(self.job_wrapper, job)
        self.jobs[future] = job
        future.add_done_callback(
            functools.partial(
                self._on_job_completed, loop=asyncio.get_running_loop()
            )
        )
        return future

    def job_wrapper(self, job: QueuedJob):
        """
        This is the function that is actually started by the thread pool
        executor. It's responsible for setting up necessary limits,
        running the job, and returning the result.
        """
        func, kwargs = Executor.get_function_and_kwargs(job)
        if asyncio.iscoroutinefunction(func):
            raise ValueError(
                f"Function {job.func!r} is an async function, which is not"
                f" supported by the {self.__class__.__name__!r} executor. Use"
                f" the AsyncExecutor instead."
            )

        time_limit = next(
            (
                limit.value
                for limit in job.limits
                if limit.type_ == Limit.Type.TIME
            ),
            None,
        )

        if time_limit:
            timer = threading.Timer(time_limit, self._timeout_handler)
            timer.start()
            try:
                func(**kwargs)
            finally:
                timer.cancel()
        else:
            func(**kwargs)

    @staticmethod
    def _timeout_handler():
        raise TimeoutError("Job timed out.")

    def _on_job_completed(
        self, future: Future, loop: asyncio.AbstractEventLoop
    ):
        job = self.jobs.pop(future)

        exc = future.exception()
        if exc is None:
            exc = future.result()

        asyncio.run_coroutine_threadsafe(
            self.job_completed(job, exc),
            loop,
        )

    def __len__(self):
        return len(self.jobs)
