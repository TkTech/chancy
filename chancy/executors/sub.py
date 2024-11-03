import threading
from concurrent.futures import ThreadPoolExecutor, Future
import asyncio
from typing import Dict
import functools

try:
    # Only available in 3.14+
    from concurrent.futures import InterpreterPoolExecutor
except ImportError:
    # Available only in 3.13 with a backport.
    from interpreters_backport.concurrent.futures.interpreter import (
        InterpreterPoolExecutor,
    )

from chancy.executors.base import Executor
from chancy.job import QueuedJob, Limit


class SubInterpreterExecutor(Executor):
    """
    .. note::

        This executor is experimental and may not work as expected. The
        sub-interpreter features it is built on is experimental and only
        available in Python 3.13 (with a backport) and >3.14.

        To use with 3.13, run:

        .. code-block:: bash

            pip install chancy[sub]

    To use this executor, simply pass the import path to this class in the
    ``executor`` field of your queue configuration:

    .. code-block:: python

        async with Chancy(dsn="postgresql://localhost/postgres") as chancy:
            await chancy.declare(
                Queue(
                    name="default",
                    executor="chancy.executors.sub.SubInterpreterExecutor",
                )
            )

    It's important to note that many C-based Python libraries are not (yet)
    compatible with sub-interpreters. If you encounter issues, you may need to
    switch to a different executor.

    :param worker: The worker instance associated with this executor.
    :param queue: The queue that this executor is associated with.
    """

    def __init__(self, worker, queue):
        super().__init__(worker, queue)
        self.pool = InterpreterPoolExecutor(max_workers=queue.concurrency)
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
        try:
            time_limit = next(
                (
                    limit.value
                    for limit in job.limits
                    if limit.type_ == Limit.Type.TIME
                ),
                None,
            )

            func, kwargs = Executor.get_function_and_kwargs(job)

            if time_limit:
                timer = threading.Timer(time_limit, self._timeout_handler)
                timer.start()
                try:
                    func(**kwargs)
                finally:
                    timer.cancel()
            else:
                func(**kwargs)

        except Exception as exc:
            return exc
        return None

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
