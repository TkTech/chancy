from psycopg import AsyncCursor, sql
from psycopg.rows import DictRow, dict_row

from chancy.app import Chancy
from chancy.plugin import Plugin
from chancy.rule import JobRules, SQLAble
from chancy.utils import timed_block
from chancy.worker import Worker


class Pruner(Plugin):
    """
    A plugin that prunes stale data from the database.

    .. note::
        This plugin is enabled by default, you only need to provide it in the
        list of plugins to customize its arguments or if ``no_default_plugins``
        is set to ``True``.

    .. testcode:: pruner

        from chancy import Chancy
        from chancy.plugins.leadership import Leadership
        from chancy.plugins.pruner import Pruner

        app = Chancy("postgresql://localhost/postgres", plugins=[
            Leadership(),
            Pruner(
                (Pruner.Rules.Queue() == "default") & (Pruner.Rules.Age() > 60)
            )
        ])

    The pruner only prunes jobs that have finished, either succeeding or
    failing. Jobs that are pending, running, or waiting to be retried are
    never pruned. When the pruner runs, it will also call the
    :py:meth:`chancy.plugin.Plugin.cleanup` method on any plugins that
    implement it, allowing them to clean up any data that is no longer
    needed such as completed workflows.

    Rules
    -----

    You can use simple rules, or combine them using the ``|`` and ``&``
    operators to create complex rules. Parenthesize each comparison when
    combining rules. Age is measured from the job's creation time, not its
    completion time.

    For example, to prune jobs that are older than 60 seconds:

    .. testcode:: pruner

        age_pruner = Pruner(Pruner.Rules.Age() > 60)

    Or to prune jobs that are older than 60 seconds and are in the "default"
    queue:

    .. testcode:: pruner

        queue_pruner = Pruner(
            (Pruner.Rules.Queue() == "default") & (Pruner.Rules.Age() > 60)
        )

    Or to prune jobs that are older than 60 seconds and are in the "default"
    queue, or any finished ``myapp.tasks.update_cache`` job regardless of age:

    .. testcode:: pruner

        job_pruner = Pruner(
            ((Pruner.Rules.Queue() == "default") & (Pruner.Rules.Age() > 60)) |
            (Pruner.Rules.Job() == "myapp.tasks.update_cache")
        )

    By default, the pruner will run every 60 seconds and will remove up to
    10,000 finished jobs in a single run whose creation time is more than
    24 hours (86,400 seconds) ago. A job that takes longer than 24 hours to
    finish is eligible on the next pruning pass after completion; there is
    no additional retention period measured from completion.

    .. testcode:: pruner
        :hide:

        # Rule construction is lazy; also check SQL generation without a DB.
        for configured in (
            app.plugins[Pruner.get_identifier()],
            age_pruner, queue_pruner, job_pruner,
        ):
            configured.rule.to_sql().as_string()

        defaults = Pruner()
        assert defaults.poll_interval == 60
        assert defaults.maximum_to_prune == 10_000
        assert defaults.rule.to_sql().as_string() == (
            "EXTRACT(EPOCH FROM (NOW() - created_at)) > 86400"
        )

    .. tip::

        By default, only an Age rule will be covered by an index. If you use
        multiple rules, you may need to create additional indexes to improve
        performance on busy queues.

    :param rule: The rule that the pruner will use to match jobs.
    :param maximum_to_prune: The maximum number of jobs to prune in a single
                             run of the pruner.
    :param poll_interval: The interval in seconds between each run of the
                          pruner.
    """

    Rules = JobRules

    def __init__(
        self,
        rule: SQLAble | None = None,
        *,
        maximum_to_prune: int = 10000,
        poll_interval: int = 60,
    ):
        super().__init__()
        self.rule = (
            rule if rule is not None else self.Rules.Age() > 60 * 60 * 24
        )
        self.maximum_to_prune = maximum_to_prune
        self.poll_interval = poll_interval

    @staticmethod
    def get_identifier() -> str:
        return "chancy.pruner"

    @staticmethod
    def get_dependencies() -> list[str]:
        return ["chancy.leadership"]

    async def run(self, worker: Worker, chancy: Chancy):
        while await self.sleep(self.poll_interval):
            await self.wait_for_leader(worker)
            async with (
                chancy.pool.connection() as conn,
                conn.cursor(row_factory=dict_row) as cursor,
            ):
                with timed_block() as chancy_time:
                    rows_removed = await self.prune(chancy, cursor)
                    chancy.log.info(
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

            for plugin in chancy.plugins.values():
                rows = await plugin.cleanup(chancy)
                if rows is None:
                    continue

                chancy.log.info(
                    f"Plugin {plugin.__class__.__name__} removed {rows}"
                    f" row(s) from the database."
                )

    async def prune(self, chancy: Chancy, cursor: AsyncCursor[DictRow]) -> int:
        """
        Prune stale records from the database.

        :param chancy: The Chancy application.
        :param cursor: The database cursor to use for the operation.
        :return: The number of rows removed from the database
        """
        job_query = sql.SQL(
            """
            WITH jobs_to_prune AS (
                SELECT queue, id  
                FROM {table}
                WHERE state IN ('succeeded', 'failed')
                AND ({rule})
                LIMIT {maximum_to_prune}
            )
            DELETE FROM {table} t
            USING jobs_to_prune p 
            WHERE t.queue = p.queue AND t.id = p.id
            """
        ).format(
            table=sql.Identifier(f"{chancy.prefix}jobs"),
            rule=self.rule.to_sql(),
            maximum_to_prune=sql.Literal(self.maximum_to_prune),
        )

        await cursor.execute(job_query)
        return cursor.rowcount
