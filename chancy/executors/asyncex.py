import asyncio
from asyncio import CancelledError

from chancy import Reference
from chancy.executors.base import Executor
from chancy.job import QueuedJob, Limit


class AsyncExecutor(Executor):
    """
    An Executor which uses asyncio to run its jobs in the main event loop.

    This executor is useful for running large numbers of IO-bound jobs, as it
    can run many jobs concurrently without blocking the main event loop and
    without the high overhead of new processes or threads. However, it is not
    suitable for CPU-bound jobs, as it will block the main event loop and
    prevent other jobs & queues from running.

    To use this executor, simply pass the import path to this class in the
    ``executor`` field of your queue configuration or use the
    :class:`~chancy.app.Chancy.Executor` shortcut:

    .. code-block:: python

        async with Chancy("postgresql://localhost/postgres") as chancy:
            await chancy.declare(
                Queue(
                    name="default",
                    executor=Chancy.Executor.Async,
                )
            )
    """

    def __init__(self, worker, queue):
        super().__init__(worker, queue)
        self.jobs: dict[asyncio.Task, QueuedJob] = {}

    async def push(self, job: QueuedJob):
        job = await self.on_job_starting(job)
        task = asyncio.create_task(self._job_wrapper(job))
        self.jobs[task] = job
        task.add_done_callback(self._on_task_done)

    def _on_task_done(self, task):
        if self.free_slots == 0:
            # Executor was exactly full
            self.worker.queue_wake_events[self.queue.name].set()
        self.jobs.pop(task)

    def __len__(self):
        return len(self.jobs)

    async def _job_wrapper(self, job: QueuedJob):
        try:
            func, kwargs = Executor.get_function_and_kwargs(job)
            if not asyncio.iscoroutinefunction(func):
                raise ValueError(
                    f"Function {job.func!r} is not an async function, which is"
                    f" required for the AsyncExecutor. Please use the"
                    f" ThreadedExecutor or ProcessExecutor instead."
                )

            timeout = next(
                (
                    limit.value
                    for limit in job.limits
                    if limit.type_ == Limit.Type.TIME
                ),
                None,
            )

            async with asyncio.timeout(timeout):
                result = await func(**kwargs)
            await self.on_job_completed(job=job, result=result)
        except (Exception, CancelledError) as exc:
            await self.on_job_completed(job=job, exc=exc, result=None)

    async def cancel(self, ref: Reference):
        for task, job in self.jobs.items():
            if job.id == ref.identifier:
                task.cancel()
                return

    async def stop(self):
        """
        Stop the executor, giving it a chance to clean up any resources it
        may have allocated to running jobs.
        """
        for task in self.jobs:
            task.cancel()
        await asyncio.gather(*self.jobs)

    def get_default_concurrency(self):
        """
        Get the default concurrency level for this executor.

        This method is called when the queue's concurrency level is set to
        None. It should return the number of jobs that can be processed
        concurrently by this executor.

        By default, returns 100.
        """
        return 100
