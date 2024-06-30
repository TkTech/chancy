import asyncio
from psycopg import AsyncConnection
from psycopg import sql

from chancy.app import Chancy, Worker
from chancy.plugins.plugin import Plugin, PluginScope
from chancy.plugins.rule import RuleT, AgeRule
from chancy.logger import logger, PrefixAdapter


class Pruner(Plugin):
    """
    A plugin that prunes jobs from the database.

    The pruner is fairly configurable, and can be used to remove jobs that are
    older than a certain age, or that match certain criteria.

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

    :param rule: The rule that the pruner will use to match jobs.
    :param maximum_to_prune: The maximum number of jobs to prune in a single
                             run of the pruner.
    :param poll_interval: The interval in seconds between each run of the
                          pruner.
    """

    def __init__(
        self,
        rule: RuleT = AgeRule(60),
        *,
        maximum_to_prune: int = 10000,
        poll_interval: int = 60 * 1,
    ):
        super().__init__()
        self.rule = rule
        self.maximum_to_prune = maximum_to_prune
        self.poll_interval = poll_interval

    @classmethod
    def get_scope(cls) -> PluginScope:
        return PluginScope.WORKER

    async def run(self, worker: Worker, chancy: Chancy):
        log = PrefixAdapter(logger, {"prefix": "Pruner"})

        while not await self.is_cancelled():
            await asyncio.sleep(self.poll_interval)
            if not worker.is_leader:
                log.debug(
                    "Skipping pruner run because this worker is not the leader."
                )
                continue

            async with chancy.pool.connection() as conn:
                log.debug(
                    "Beginning pruner run to remove jobs from the database."
                )
                rows_removed = await self.prune(chancy, conn)
                log.info(
                    f"Pruner removed {rows_removed} row(s) from the database."
                )

    async def prune(self, chancy: Chancy, conn: AsyncConnection) -> int:
        """
        Prune jobs from the database according to the configured rule.

        :param chancy: The Chancy application.
        :param conn: The database connection.
        :return: The number of rows removed from the database
        """
        query = sql.SQL(
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

        async with conn.cursor() as cursor:
            async with conn.transaction():
                await cursor.execute(query)
                return cursor.rowcount
