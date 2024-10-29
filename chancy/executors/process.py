import asyncio
import functools
import os
import resource
import signal
import threading
from asyncio import Future
from concurrent.futures import ProcessPoolExecutor
from typing import Callable

from chancy.executors.base import Executor
from chancy.job import QueuedJob, Limit


class _TimeoutThread(threading.Thread):
    """
    A thread that will raise a TimeoutError after a specified number of
    seconds.
    """

    def __init__(self, timeout: int, cancel: threading.Event):
        super().__init__()
        self.timeout = timeout
        self.cancel = cancel

    def run(self):
        if self.cancel.wait(self.timeout) is True:
            # Our call to wait() returning True means that the flag was set
            # before the timeout elapsed, so we should cancel our alarm.
            return

        # If we reach this point, the timeout has elapsed, and we should raise
        # a TimeoutError back in the main process thread.
        os.kill(os.getpid(), signal.SIGALRM)


class ProcessExecutor(Executor):
    """
    .. note::

        Currently, this is the default executor used by Chancy.

    An Executor which uses a process pool to run its jobs.

    This executor is useful for running jobs that are CPU-bound, avoiding the
    GIL (Global Interpreter Lock) that Python uses to ensure thread safety.

    If a timeout is requested for a job, a separate thread will be spawned
    within each process to raise a TimeoutError if the job takes too long to
    complete.

    To use this executor, simply pass the import path to this class in the
    ``executor`` field of your queue configuration:

    .. code-block:: python

        async with Chancy(dsn="postgresql://localhost/postgres") as chancy:
            await chancy.declare(
                Queue(
                    name="default",
                    executor="chancy.executors.process.ProcessExecutor"
                )
            )

    :param queue: The queue that this executor is associated with.
    :param maximum_jobs_per_worker: The maximum number of jobs that each worker
                                    can run before being replaced.
    """

    def __init__(self, worker, queue, *, maximum_jobs_per_worker: int = 100):
        super().__init__(worker, queue)

        self.processes: dict[Future, QueuedJob] = {}
        self.pool = ProcessPoolExecutor(
            max_workers=queue.concurrency,
            max_tasks_per_child=maximum_jobs_per_worker,
            initializer=self.on_initialize_worker,
        )

    @staticmethod
    def on_initialize_worker():
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

    async def push(self, job: QueuedJob) -> Future:
        future: Future = self.pool.submit(self.job_wrapper, job)
        future.add_done_callback(
            functools.partial(
                self._on_job_completed, loop=asyncio.get_running_loop()
            )
        )
        self.processes[future] = job
        return future

    def __len__(self):
        return len(self.processes)

    @classmethod
    def job_wrapper(cls, job: QueuedJob):
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

        for limit in job.limits:
            match limit.type_:
                case Limit.Type.TIME:
                    # We need this to work on platforms that don't support
                    # SIGALRM (looking at you, Windows). So we use a separate
                    # thread to raise a TimeoutError if the job takes too long.
                    signal.signal(signal.SIGALRM, cls.job_signal_handler)
                    cancel = threading.Event()
                    timeout_thread = _TimeoutThread(limit.value, cancel)
                    timeout_thread.start()
                    cleanup.append(
                        lambda: cancel.set() and timeout_thread.join()
                    )
                case Limit.Type.MEMORY:
                    previous_soft, _ = resource.getrlimit(resource.RLIMIT_AS)
                    resource.setrlimit(resource.RLIMIT_AS, (limit.value, -1))
                    cleanup.append(
                        lambda: resource.setrlimit(
                            resource.RLIMIT_AS, (previous_soft, -1)
                        )
                    )

        func, kwargs = Executor.get_function_and_kwargs(job)

        try:
            func(**kwargs)
        finally:
            for clean in cleanup:
                clean()

        return job

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
        # If we receive a SIGALRM signal, we should raise a TimeoutError, as
        # this is our _TimeoutThread telling us that the job has taken too long
        # to complete.
        if signum == signal.SIGALRM:
            raise TimeoutError("Job timed out.")

    def _on_job_completed(
        self, future: Future, loop: asyncio.AbstractEventLoop
    ):
        job = self.processes.pop(future)
        exc = future.exception()
        if exc is None:
            job = future.result()

        f = asyncio.run_coroutine_threadsafe(
            self.job_completed(job, exc),
            loop,
        )
        f.result()
