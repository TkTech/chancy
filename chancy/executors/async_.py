import asyncio
import dataclasses
import traceback
from asyncio import Future
from typing import Dict
from datetime import datetime, timezone

from chancy.executor import Executor
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

    :param queue: The queue that this executor is associated with.
    """

    def __init__(self, queue):
        super().__init__(queue)
        self.running_jobs: Dict[str, asyncio.Task] = {}

    async def push(self, job: JobInstance) -> Future:
        task = asyncio.create_task(self.run_job(job))
        self.running_jobs[job.id] = task
        return task

    def __len__(self):
        return len(self.running_jobs)

    async def run_job(self, job: JobInstance):
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
                result = await asyncio.wait_for(func(**kwargs), timeout=timeout)
            except (asyncio.TimeoutError, TimeoutError):
                raise asyncio.TimeoutError(
                    f"Job {job.id} timed out after {timeout} seconds"
                )

            await self.job_completed(job, result, None)
            return result
        except Exception as exc:
            await self.job_completed(job, None, exc)
            raise
        finally:
            del self.running_jobs[job.id]

    async def job_completed(
        self, job: JobInstance, result: any, exc: Exception | None
    ):
        """
        Called when a job has completed.

        This method should be called by the executor when a job has completed
        execution. It will update the job's state in the queue and handle
        retries if necessary.
        """
        if exc is not None:
            traceback.print_exception(type(exc), exc, exc.__traceback__)
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
            now = datetime.now(tz=timezone.utc)
            new_state = dataclasses.replace(
                job,
                state=JobInstance.State.SUCCEEDED,
                completed_at=now,
            )

        await self.queue.push_job_update(new_state)
