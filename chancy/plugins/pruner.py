import time
from datetime import datetime, timezone, timedelta

from psycopg import sql

from chancy.hub import Hub
from chancy.plugin import Plugin
from chancy.worker import Worker
from chancy.logger import PrefixAdapter, logger


class Pruner(Plugin):
    """
    The Pruner plugin is responsible for removing jobs that have been
    completed (successfully or not) for a certain amount of time.

    This plugin only works when the cluster has an active leader.

    .. code-block:: python

        async with Chancy(
            dsn="postgresql://username:password@localhost:8190/postgres",
            queues=[
                Queue(name="default", concurrency=1),
            ],
            plugins=[
                Pruner(interval=60, rules={
                    "default": 60 * 60 * 24,
                }),
            ],
        ) as app:
            await Worker(app).start()

    :param interval: The minimum interval in seconds between pruning checks.
    :param rules: A dictionary of queue names and the time in seconds
                  after which a completed job is pruned.
    """

    def __init__(self, *, interval: int, rules: dict[str, int]):
        self.interval = interval
        self.rules = rules
        self._last_check = 0

    @classmethod
    def get_type(cls):
        return cls.Type.LEADER

    async def on_startup(self, hub: Hub):
        hub.on(Worker.Event.ON_LEADERSHIP_LOOP, self._recover_jobs)

    async def on_shutdown(self, hub: Hub):
        hub.remove(Worker.Event.ON_LEADERSHIP_LOOP, self._recover_jobs)

    async def _recover_jobs(self, worker: Worker):
        prune_logger = PrefixAdapter(logger, {"prefix": "PRUNE"})

        if time.monotonic() - self._last_check < self.interval:
            return

        self._last_check = time.monotonic()

        async with worker.app.pool.connection() as conn:
            async with conn.transaction():
                prune_logger.debug("Checking for stale completed jobs...")
                for queue_name, remove_after in self.rules.items():
                    threshold = datetime.now(tz=timezone.utc) - timedelta(
                        seconds=remove_after
                    )
                    await conn.execute(
                        sql.SQL(
                            """
                            DELETE FROM {jobs}
                            WHERE state IN ('completed', 'failed')
                            AND completed_at < %(threshold)s
                            AND queue = %(queue_name)s
                            """
                        ).format(
                            jobs=sql.Identifier(f"{worker.app.prefix}jobs"),
                        ),
                        {"threshold": threshold, "queue_name": queue_name},
                    )
                prune_logger.debug("Pruning completed jobs done.")
