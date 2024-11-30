import dataclasses
from datetime import datetime, timedelta, timezone
import random

from chancy.plugin import Plugin, PluginScope
from chancy.job import QueuedJob
from chancy.worker import Worker


class RetryPlugin(Plugin):
    """
    Plugin that handles job retries based on settings stored in the Job's
    metadata.

    This plugin can be used as an example for implementing your own retry
    policies.

    Usage:

    .. code-block:: python

        from chancy import job
        from chancy.plugins.retry import RetryPlugin

        @job()
        def job_that_fails():
            raise ValueError("This job should fail.")

        async with Chancy(..., plugins=[RetryPlugin()]) as chancy:
            await chancy.declare(Queue("default"))
            await chancy.push(
                job_that_fails.job.with_max_attempts(3).with_meta({
                    "retry_settings": {
                        "backoff": 2,
                        "backoff_factor": 3,
                        "backoff_limit": 300,
                        "backoff_jitter": [1, 5],
                    }
                })
            )

    The above example will retry the job 3 times, with a starting backoff of 2
    seconds, a backoff factor of 3, a backoff limit of 300 seconds, and a
    random jitter of between 1 and 5 seconds.
    """

    @classmethod
    def get_scope(cls) -> PluginScope:
        return PluginScope.WORKER

    @staticmethod
    def calculate_next_run(job: QueuedJob, retry_settings: dict) -> datetime:
        delay = retry_settings.get("backoff", 1)
        factor = retry_settings.get("backoff_factor", 2.0)
        limit = retry_settings.get("backoff_limit", 300)
        jitter = retry_settings.get("backoff_jitter", [1, 5])

        delay *= factor ** (job.attempts - 1)
        delay = min(delay, limit)
        delay += random.uniform(*jitter)

        return datetime.now(timezone.utc) + timedelta(seconds=delay)

    async def on_job_completed(
        self, job: QueuedJob, worker: Worker, *, exc: Exception | None = None
    ) -> QueuedJob:
        if exc is None or job.state not in {
            job.State.FAILED,
            job.State.RETRYING,
        }:
            return job

        if job.attempts >= job.max_attempts:
            return job

        retry_settings = job.meta.get("retry_settings", {})

        # We don't need to adjust the # of attempts as the base executor will
        # do that for us.
        return dataclasses.replace(
            job,
            state=QueuedJob.State.RETRYING,
            scheduled_at=self.calculate_next_run(job, retry_settings),
            completed_at=None,
        )
