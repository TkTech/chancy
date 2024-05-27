import sentry_sdk

from chancy.hub import Hub
from chancy.plugin import Plugin
from chancy.worker import Worker
from chancy.app import JobContext


class Sentry(Plugin):
    """
    The Sentry plugin provides integration with Sentry for error tracking.

    While you can use sentry as you would normally in any other application,
    this plugin adds additional context to the Sentry events, such as the
    worker_id and the job_id.

    .. code-block:: python

        import sentry_sdk
        sentry_sdk.init(dsn="https://your-sentry-dsn")

        async with Chancy(
            dsn="postgresql://username:password@localhost:8190/postgres",
            queues=[
                Queue(name="default", concurrency=1),
            ],
            plugins=[
                Sentry(),
            ],
        ) as app:
            await Worker(app).start()
    """

    @classmethod
    def get_type(cls):
        return cls.Type.WORKER

    async def on_startup(self, hub: Hub):
        hub.on(Worker.Event.ON_EXCEPTION, self._handle_exception)

    async def on_shutdown(self, hub: Hub):
        hub.remove(Worker.Event.ON_EXCEPTION, self._handle_exception)

    @staticmethod
    async def _handle_exception(
        worker: Worker, exc: Exception, context: JobContext
    ):
        with sentry_sdk.push_scope() as scope:
            scope.set_tag("worker_id", worker.worker_id)
            scope.set_context(
                "job",
                {
                    "id": context.id,
                    "attempts": context.attempts,
                    "started_at": context.started_at,
                    "completed_at": context.completed_at,
                    "state": context.state,
                },
            )
            sentry_sdk.capture_exception(exc)
