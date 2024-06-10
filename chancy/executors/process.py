import asyncio
import dataclasses
import functools
import os
import resource
import signal
import typing
import threading
from asyncio import Future
from concurrent.futures import ProcessPoolExecutor
from typing import Callable

from chancy.executor import Executor, JobInstance, Limit

if typing.TYPE_CHECKING:
    from chancy.queue import Queue


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
    An Executor which uses a process pool to run its jobs.

    This executor is useful for running jobs that are CPU-bound, avoiding the
    GIL (Global Interpreter Lock) that Python uses to ensure thread safety.

    If a timeout is requested for a job, a separate thread will be spawned
    within each process to raise a TimeoutError if the job takes too long to
    complete.

    .. note::

        Currently, this is the default executor used by Chancy.

    :param queue: The queue that this executor is associated with.
    :param maximum_jobs_per_worker: The maximum number of jobs that each worker
                                    can run before being replaced.
    """

    def __init__(self, queue: "Queue", *, maximum_jobs_per_worker: int = 100):
        super().__init__(queue)

        self.processes: dict[Future, JobInstance] = {}
        self.pool = ProcessPoolExecutor(
            max_workers=self.queue.concurrency,
            max_tasks_per_child=maximum_jobs_per_worker,
        )

    async def push(self, job: JobInstance) -> Future:
        future: Future = self.pool.submit(self.job_wrapper, job)
        future.add_done_callback(
            functools.partial(
                self.job_completed, loop=asyncio.get_running_loop()
            )
        )
        self.processes[future] = job
        return future

    def __len__(self):
        return len(self.processes)

    @classmethod
    def job_wrapper(cls, job: JobInstance):
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

        mod_name, func_name = job.func.rsplit(".", 1)
        mod = __import__(mod_name, fromlist=[func_name])
        func = getattr(mod, func_name)

        kwargs = job.kwargs or {}

        try:
            func(**kwargs)
        finally:
            for clean in cleanup:
                clean()

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

    def job_completed(self, future: Future, loop: asyncio.AbstractEventLoop):
        """
        Handles the completion of a job future.

        This method is called when a job future completes, and is responsible
        for cleaning up the job and notifying the queue that the job has
        completed.
        """
        job = self.processes.pop(future)
        exc = future.exception()

        if exc is not None:
            new_state = dataclasses.replace(
                job,
                state=(
                    JobInstance.State.FAILED
                    if job.attempts + 1 >= job.max_attempts
                    else JobInstance.State.RETRYING
                ),
                attempts=job.attempts + 1,
            )
        else:
            new_state = dataclasses.replace(
                job,
                state=JobInstance.State.SUCCEEDED,
                completed_at=job.started_at,
            )

        f = asyncio.run_coroutine_threadsafe(
            self.queue.push_job_update(new_state),
            loop,
        )
        f.result()
