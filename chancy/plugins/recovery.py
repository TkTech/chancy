from psycopg import AsyncCursor
from psycopg import sql

from chancy.app import Chancy
from chancy.worker import Worker
from chancy.utils import timed_block
from chancy.plugin import Plugin, PluginScope


class Recovery(Plugin):
    """
    Recovers jobs that appear to be abandoned by a worker.

    Typically, this happens when a worker is unexpectedly terminated, or has
    otherwise been lost which we recognize by checking the last seen timestamp
    of the worker heartbeat.

    This will transition any matching jobs back to the "pending" state, and
    increment the `max_attempts` counter by 1 to allow it to be retried.

    :param poll_interval: The number of seconds between recovery poll intervals.
    """

    def __init__(self, *, poll_interval: int = 60):
        super().__init__()
        self.poll_interval = poll_interval

    @classmethod
    def get_scope(cls) -> PluginScope:
        return PluginScope.WORKER

    async def run(self, worker: Worker, chancy: Chancy):
        while await self.sleep(self.poll_interval):
            await self.wait_for_leader(worker)
            async with chancy.pool.connection() as conn:
                async with conn.cursor() as cursor:
                    with timed_block() as chancy_time:
                        rows_recovered = await self.recover(
                            worker, chancy, cursor
                        )
                        chancy.log.info(
                            f"Recovery recovered {rows_recovered} row(s) from"
                            f" the database. Took {chancy_time.elapsed:.2f}"
                            f" seconds."
                        )
                        await chancy.notify(
                            cursor,
                            "recovery.recovered",
                            {
                                "elapsed": chancy_time.elapsed,
                                "rows_recovered": rows_recovered,
                            },
                        )

    @classmethod
    async def recover(
        cls, worker: Worker, chancy: Chancy, cursor: AsyncCursor
    ) -> int:
        """
        Recover jobs that were running when the worker was unexpectedly
        terminated, or has otherwise been lost.

        :param worker: The worker that is running the recovery.
        :param chancy: The Chancy application.
        :param cursor: The cursor to use for database operations.
        :return: The number of rows recovered from the database
        """
        query = sql.SQL(
            """
            UPDATE
                {jobs} cj
            SET
                state = 'pending',
                taken_by = NULL,
                started_at = NULL,
                max_attempts = max_attempts + 1
            WHERE
               NOT EXISTS (
                    SELECT 1
                    FROM {workers} cw
                    WHERE (
                        cw.worker_id = cj.taken_by
                        AND
                        cw.last_seen >= NOW() - INTERVAL '{interval} SECOND'
                    )
              )
              AND state = 'running';
        """
        ).format(
            jobs=sql.Identifier(f"{chancy.prefix}jobs"),
            workers=sql.Identifier(f"{chancy.prefix}workers"),
            interval=sql.Literal(worker.heartbeat_timeout),
        )

        await cursor.execute(query)
        return cursor.rowcount
