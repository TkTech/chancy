import asyncio

from chancy.executors.base import Executor
from chancy.job import JobInstance, Limit
from chancy.utils import import_string


class AsyncExecutor(Executor):
    """
    An Executor which uses asyncio to run its jobs in the main event loop.

    This executor is useful for running large numbers of IO-bound jobs, as it
    can run many jobs concurrently without blocking the main event loop and
    without the high overhead of new processes or threads. However, it is not
    suitable for CPU-bound jobs, as it will block the main event loop and
    prevent other jobs & queues from running.
    """

    def __init__(self, worker, queue):
        super().__init__(worker, queue)
        self.running_jobs: set[asyncio.Task] = set()

    async def push(self, job: JobInstance):
        task = asyncio.create_task(self._job_wrapper(job))
        task.add_done_callback(self._job_cleanup)
        self.running_jobs.add(task)

    def __len__(self):
        return len(self.running_jobs)

    def _job_cleanup(self, task: asyncio.Task):
        self.running_jobs.discard(task)
        task.exception()

    async def _job_wrapper(self, job: JobInstance):
        try:
            func = import_string(job.func)
            if not asyncio.iscoroutinefunction(func):
                raise ValueError(
                    f"Function {job.func} is not a coroutine function"
                )

            kwargs = job.kwargs or {}

            timeout = next(
                (
                    limit.value
                    for limit in job.limits
                    if limit.type_ == Limit.Type.TIME
                ),
                None,
            )

            try:
                # This annoyingly creates quite the excessive traceback,
                # we should revisit this and clean it up.
                await asyncio.wait_for(func(**kwargs), timeout=timeout)
            except (asyncio.TimeoutError, TimeoutError):
                raise asyncio.TimeoutError(
                    f"Job {job.id} timed out after {timeout} seconds"
                )
            self.worker.manager.add(self.job_completed(job))
        except Exception as exc:
            self.worker.manager.add(self.job_completed(job, exc))
