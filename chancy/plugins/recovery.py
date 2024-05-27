import time
from datetime import datetime, timezone, timedelta

from psycopg import sql

from chancy.hub import Hub
from chancy.plugin import Plugin
from chancy.worker import Worker
from chancy.logger import PrefixAdapter, logger


class Recovery(Plugin):
    """
    The Recovery plugin is responsible for recovering jobs that have gotten
    stuck in a running state due to a worker crashing or going offline.

    This plugin only works when the cluster has an active leader.

    :param interval: The minimum interval in seconds between recovery checks.
    :param rescue_after: The minimum time a worker must be offline before
                         its jobs are recovered.
    """

    def __init__(self, *, interval: int, rescue_after: int):
        self.interval = interval
        self.rescue_after = rescue_after
        self._last_check = 0

    @classmethod
    def get_type(cls):
        return cls.Type.LEADER

    async def on_startup(self, hub: Hub):
        hub.on(Worker.Event.ON_LEADERSHIP_LOOP, self._recover_jobs)

    async def on_shutdown(self, hub: Hub):
        hub.remove(Worker.Event.ON_LEADERSHIP_LOOP, self._recover_jobs)

    async def _recover_jobs(self, worker: Worker):
        recovery_logger = PrefixAdapter(logger, {"prefix": "RECOVERY"})

        if time.monotonic() - self._last_check < self.interval:
            return

        self._last_check = time.monotonic()

        async with worker.app.pool.connection() as conn:
            recovery_logger.debug("Checking for stuck jobs...")

            threshold = datetime.now(tz=timezone.utc) - timedelta(
                seconds=self.rescue_after
            )

            async with conn.transaction():
                await conn.execute(
                    sql.SQL(
                        """
                        WITH living_workers AS (
                            SELECT
                                worker_id,
                                last_seen
                            FROM
                                {workers}
                            WHERE
                                last_seen > %(threshold)s
                        )
                        UPDATE
                            {jobs}
                        SET
                            state = 'pending',
                            started_at = NULL,
                            taken_by = NULL
                        WHERE
                            state = 'running'
                        AND
                            taken_by NOT in (
                                SELECT worker_id FROM living_workers
                            )
                        """
                    ).format(
                        workers=sql.Identifier(f"{worker.app.prefix}workers"),
                        jobs=sql.Identifier(f"{worker.app.prefix}jobs"),
                    ),
                    {"threshold": threshold},
                )

                recovery_logger.debug("Recovered stuck jobs.")
