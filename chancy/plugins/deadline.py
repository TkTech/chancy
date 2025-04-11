__all__ = ("Deadline",)
from psycopg import AsyncCursor, sql
from psycopg.rows import DictRow, dict_row

from chancy.app import Chancy
from chancy.worker import Worker
from chancy.job import QueuedJob
from chancy.plugin import Plugin
from chancy.utils import timed_block


class Deadline(Plugin):
    """
    The Deadline plugin is responsible for cleaning up jobs which have reached
    their configured deadline without being picked up or completed.

    .. note::

        This plugin is enabled by default, you only need to provide it in the
        list of plugins to customize its arguments or if ``no_default_plugins``
        is set to ``True``.

    .. code-block:: python

        from chancy.plugins.deadline import Deadline

        async with Chancy(..., plugins=[
            Deadline(maximum_to_prune=1000, poll_interval=60),
        ]) as chancy:
            ...

    :param maximum_to_prune: The maximum number of jobs to prune at once.
    :param poll_interval: The number of seconds between deadline checks.
    """

    def __init__(
        self, *, maximum_to_prune: int = 10000, poll_interval: int = 60
    ):
        super().__init__()
        self.maximum_to_prune = maximum_to_prune
        self.poll_interval = poll_interval

    @staticmethod
    def get_identifier() -> str:
        return "chancy.deadline"

    async def run(self, worker: Worker, chancy: Chancy):
        """
        Run the deadline plugin.

        This function will run indefinitely, checking for expired jobs and
        transitioning them to the expired state.
        """
        while await self.sleep(self.poll_interval):
            async with chancy.pool.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cursor:
                    with timed_block() as chancy_time:
                        rows_expired = await self.transition_expired(
                            chancy, cursor
                        )
                        chancy.log.info(
                            f"Deadline plugin transitioned {rows_expired}"
                            f" job(s) to the expired state. Took"
                            f" {chancy_time.elapsed:.2f} seconds."
                        )
                        await chancy.notify(
                            cursor,
                            "deadline.expired",
                            {
                                "elapsed": chancy_time.elapsed,
                                "rows_expired": rows_expired,
                            },
                        )

    async def transition_expired(
        self, chancy: Chancy, cursor: AsyncCursor[DictRow]
    ) -> int:
        """
        Transition expired jobs to the expired state.

        :param chancy: The Chancy instance.
        :param cursor: The database cursor.
        :return: The number of jobs transitioned.
        """
        await cursor.execute(
            sql.SQL(
                """
                UPDATE {table}
                SET state = 'expired'
                WHERE id = (
                    SELECT id
                    FROM {table}
                    WHERE state IN ('pending', 'retrying')
                    AND deadline < NOW()
                    LIMIT %(maximum_to_prune)s
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING *
                """
            ).format(
                table=sql.Identifier(f"{chancy.prefix}jobs"),
            ),
            {
                "maximum_to_prune": self.maximum_to_prune,
            },
        )
        result_count = cursor.rowcount
        async for row in cursor:
            try:
                await self.on_job_expired(chancy, QueuedJob.unpack(row))
            except NotImplementedError:
                # If the plugin doesn't implement on_job_expired, we
                # short-circuit to avoid unnecessary processing.
                return result_count
        return result_count

    async def on_job_expired(self, chancy: Chancy, job: QueuedJob):
        """
        Called when a job has expired.

        Subclasses should implement this method to implement custom logic
        when a job has expired, such as sending notifications or moving it
        to another queue.

        :param chancy: The Chancy instance.
        :param job: The job that has expired.
        """
        raise NotImplementedError()
