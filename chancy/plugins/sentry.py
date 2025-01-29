from typing import Any

import sentry_sdk

from chancy.job import QueuedJob
from chancy.worker import Worker
from chancy.plugin import Plugin


class SentryPlugin(Plugin):
    """
    A plugin that sends errors to Sentry.

    This plugin will send all errors that occur during job execution to Sentry,
    along with extended metadata about the job such as the executing worker,
    the queue, and the job ID.

    This plugin assumes you've already configured sentry-sdk using
    ``sentry_sdk.init()`` at some point, to prevent conflicts with other Sentry
    integrations.

    Requires the sentry-sdk package version 2.0.0 or higher.

    Usage:

    .. code-block:: bash

        pip install sentry-sdk

    .. code-block:: python

        from chancy.plugins.sentry import SentryPlugin

        async with Chancy(..., plugins=[SentryPlugin()]) as chancy:
            pass
    """

    async def on_job_completed(
        self,
        *,
        worker: Worker,
        job: QueuedJob,
        exc: Exception | None = None,
        result: Any | None = None,
    ) -> QueuedJob:
        if exc:
            with sentry_sdk.new_scope() as scope:
                scope.set_tags(
                    {
                        "chancy.func": job.func,
                        "chancy.worker": worker.worker_id,
                        "chancy.queue": job.queue,
                    }
                )
                scope.set_extra("chancy.job_id", job.id)
                scope.capture_exception(exc)
        return job
