from psycopg import AsyncCursor
from psycopg import sql

from chancy.app import Chancy
from chancy.worker import Worker
from chancy.plugin import Plugin, PluginScope
from chancy.plugins.rule import Age, SQLAble
from chancy.utils import timed_block


class Pruner(Plugin):
    """
    A plugin that prunes stale data from the database.

    Jobs
    ----

    The pruner will never prune jobs that haven't been run yet ("pending"),
    or are currently being run ("running").

    You can use simple rules, or combine them using the `|` and `+` operators
    to create complex rules.

    For example, to prune jobs that are older than 60 seconds:

    .. code-block:: python

        from chancy.plugins.rule import AgeRule
        Pruner(AgeRule(60))

    Or to prune jobs that are older than 60 seconds and are in the "default"
    queue:

    .. code-block:: python

        from chancy.plugins.rule import AgeRule, QueueRule
        Pruner(QueueRule("default") + AgeRule(60))

    Or to prune jobs that are older than 60 seconds and are in the "default"
    queue, or instantly deleted if the job is `update_cache`:

    .. code-block:: python

        from chancy.plugins.rule import AgeRule, QueueRule, JobRule
        Pruner((QueueRule("default") + AgeRule(60)) | JobRule("update_cache"))

    By default, the pruner will run every 60 seconds and will remove up to
    10,000 jobs in a single run that have been completed for more than 60
    seconds.

    .. note::

        By default, only an AgeRule will be covered by an index. If you use
        multiple rules, you may need to create additional indexes to improve
        performance on busy queues.

    Events
    ------

    The following events are emitted by the pruner plugin:

    +---------------------+------------------------------------------------+
    | Event Name          | Description                                    |
    +=====================+================================================+
    | `pruner.removed`    | Emitted when the pruner has removed jobs from  |
    |                     | the database.                                  |
    +---------------------+------------------------------------------------+

    :param rule: The rule that the pruner will use to match jobs.
    :param maximum_to_prune: The maximum number of jobs to prune in a single
                             run of the pruner.
    :param poll_interval: The interval in seconds between each run of the
                          pruner.
    """

    def __init__(
        self,
        rule: SQLAble = Age() > 60,
        *,
        maximum_to_prune: int = 10000,
        poll_interval: int = 60,
    ):
        super().__init__()
        self.rule = rule
        self.maximum_to_prune = maximum_to_prune
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
                        rows_removed = await self.prune(chancy, cursor)
                        self.log.info(
                            f"Pruner removed {rows_removed} row(s) from the"
                            f" database. Took {chancy_time.elapsed:.2f}"
                            f" seconds."
                        )
                        await chancy.notify(
                            cursor,
                            "pruner.removed",
                            {
                                "elapsed": chancy_time.elapsed,
                                "rows_removed": rows_removed,
                            },
                        )

    async def prune(self, chancy: Chancy, cursor: AsyncCursor) -> int:
        """
        Prune stale records from the database.

        :param chancy: The Chancy application.
        :param cursor: The database cursor to use for the operation.
        :return: The number of rows removed from the database
        """
        job_query = sql.SQL(
            """
            WITH jobs_to_prune AS (
                SELECT ctid
                FROM {table}
                WHERE state NOT IN ('pending', 'running')
                AND ({rule})
                LIMIT {maximum_to_prune}
            )
            DELETE FROM {table}
            WHERE ctid IN (SELECT ctid FROM jobs_to_prune)
            """
        ).format(
            table=sql.Identifier(f"{chancy.prefix}jobs"),
            rule=self.rule.to_sql(),
            maximum_to_prune=sql.Literal(self.maximum_to_prune),
        )

        await cursor.execute(job_query)
        return cursor.rowcount
