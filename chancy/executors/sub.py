import os
import asyncio
import threading
import functools
from concurrent.futures import Future

import sys
from typing import Any

from chancy.worker import Worker
from chancy.queue import Queue

try:
    # Only available in 3.14+
    from concurrent.futures import InterpreterPoolExecutor
except ImportError:
    # Available only in 3.13 with a backport.
    try:
        from interpreters_backport.concurrent.futures.interpreter import (
            InterpreterPoolExecutor,
        )
    except ImportError:
        raise ImportError(
            "The SubInterpreterExecutor requires Python 3.13 or later."
        )

from chancy.executors.base import Executor, ConcurrentExecutor
from chancy.job import QueuedJob, Limit


class SubInterpreterExecutor(ConcurrentExecutor):
    """
    .. note::

        This executor is experimental and may not work as expected. The
        sub-interpreter features it is built on is experimental and only
        available in Python 3.13 (with a backport) and >3.14.

        To use with 3.13, run:

        .. code-block:: bash

            pip install chancy[sub]

    To use this executor, simply pass the import path to this class in the
    ``executor`` field of your queue configuration or use the
    :class:`~chancy.app.Chancy.Executor` shortcut:

    .. code-block:: python

        async with Chancy("postgresql://localhost/postgres") as chancy:
            await chancy.declare(
                Queue(
                    name="default",
                    executor=Chancy.Executor.SubInterpreter,
                )
            )

    It's important to note that many C-based Python libraries are not (yet)
    compatible with sub-interpreters. If you encounter issues, you may need to
    switch to a different executor.

    :param worker: The worker instance associated with this executor.
    :param queue: The queue that this executor is associated with.
    """

    def __init__(self, worker: Worker, queue: Queue):
        super().__init__(worker, queue)
        self.pool = InterpreterPoolExecutor(
            max_workers=queue.concurrency,
            initializer=self.on_initialize_worker,
            initargs=(sys.path,),
        )

    @staticmethod
    def on_initialize_worker(parent_sys_path: list[str]):
        """
        This method is called in each worker before it begins running jobs.
        It can be used to perform any necessary setup, such as loading NLTK
        datasets or calling ``django.setup()``.

        By default, it replaces the running job's ``sys.path`` with the workers.
        """
        # Unlike all other executors, the InterpreterPoolExecutor does not
        # automatically inherit the parent process's sys.path. This is a
        # workaround to ensure that the worker has the same sys.path as the
        # parent process or tests will fail.
        sys.path = parent_sys_path

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

    @classmethod
    def job_wrapper(cls, job: QueuedJob) -> tuple[QueuedJob, Any]:
        """
        This is the function that is actually started by the sub-interpreter
        executor. It's responsible for setting up necessary limits,
        running the job, and returning the result.
        """
        func, kwargs = Executor.get_function_and_kwargs(job)

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
            timer = threading.Timer(time_limit, cls._timeout_handler)
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
