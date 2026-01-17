from abc import ABCMeta

from psycopg import AsyncCursor, sql
from psycopg.rows import DictRow, dict_row

from chancy.app import Chancy
from chancy.plugin import Plugin
from chancy.rule import ConcurrencyRules, JobRules, SQLAble
from chancy.utils import timed_block
from chancy.worker import Worker


class PrunerMeta(ABCMeta):
    """Metaclass to handle deprecated Rules attribute."""

    def __getattr__(cls, name):
        if name == "Rules":
            import warnings

            warnings.warn(
                "Pruner.Rules is deprecated. Use Pruner.JobRules instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            return cls.JobRules
        raise AttributeError(f"'{cls.__name__}' has no attribute '{name}'")


class Pruner(Plugin, metaclass=PrunerMeta):
    """
    A plugin that prunes stale data from the database.

    .. note::
        This plugin is enabled by default, you only need to provide it in the
        list of plugins to customize its arguments or if ``no_default_plugins``
        is set to ``True``.

    .. code-block:: python

        from chancy.plugins.leadership import Leadership
        from chancy.plugins.pruner import Pruner

        async with Chancy(..., plugins=[
            Leadership(),
            Pruner(
                job_rule=Pruner.JobRules.Queue() == "default" & (Pruner.JobRules.Age() > 60),
                concurrency_rule=Pruner.ConcurrencyRules.Age() > 60*60*24*3
            )
        ]) as chancy:
            ...

    The pruner will never prune jobs that haven't been run yet or are currently
    running. It also cleans up stale concurrency configuration records. When
    the pruner runs, it will also call the :py:meth:`chancy.plugin.Plugin.cleanup`
    method on any plugins that implement it, allowing them to clean up any data
    that is no longer needed such as completed workflows.

    Rules
    -----

    You can use simple rules, or combine them using the ``|`` and ``&``
    operators to create complex rules.

    For example, to prune jobs that are older than 60 seconds:

    .. code-block:: python

        Pruner(job_rule=Pruner.JobRules.Age() > 60)

    Or to prune jobs that are older than 60 seconds and are in the "default"
    queue:

    .. code-block:: python

        Pruner(job_rule=Pruner.JobRules.Queue() == "default" & (Pruner.JobRules.Age() > 60))

    Or to prune jobs that are older than 60 seconds and are in the "default"
    queue, or instantly deleted if the job is `update_cache`:

    .. code-block:: python

        Pruner(
            job_rule=(Pruner.JobRules.Queue() == "default" & (Pruner.JobRules.Age() > 60)) |
                     Pruner.JobRules.Job() == "update_cache"
        )

    To customize concurrency rule cleanup:

    .. code-block:: python

        # Clean rules older than 3 days
        Pruner(concurrency_rule=Pruner.ConcurrencyRules.Age() > 60*60*24*3)

        # Clean orphaned rules and those older than 12 hours
        Pruner(concurrency_rule=(
            Pruner.ConcurrencyRules.Orphaned() |
            Pruner.ConcurrencyRules.Age() > 60*60*12
        ))

        # Disable concurrency rule cleanup
        Pruner(concurrency_rule=None)

    By default, the pruner will run every 60 seconds and will remove up to
    10,000 jobs in a single run that have been completed for more than 1 day.
    It will also clean up concurrency rules older than 3 days or that are orphaned.

    .. tip::

        By default, only an Age rule will be covered by an index. If you use
        multiple rules, you may need to create additional indexes to improve
        performance on busy queues.

    .. deprecated::
        The ``rule`` parameter is deprecated. Use ``job_rule`` instead.

    :param rule: [DEPRECATED] The rule for pruning jobs. Use ``job_rule`` instead.
    :param job_rule: The rule that the pruner will use to match jobs for pruning.
    :param concurrency_rule: The rule for pruning concurrency rules.
                                   Defaults to rules older than 7 days or orphaned.
                                   Set to None to disable.
    :param maximum_to_prune: The maximum number of jobs to prune in a single
                             run of the pruner.
    :param poll_interval: The interval in seconds between each run of the
                          pruner.
    """

    JobRules = JobRules
    ConcurrencyRules = ConcurrencyRules

    def __init__(
        self,
        rule: SQLAble | None = None,  # Deprecated
        *,
        job_rule: SQLAble | None = JobRules.Age() > 60 * 60 * 24,
        concurrency_rule: SQLAble | None = (
            (ConcurrencyRules.Age() > 60 * 60 * 24 * 7)  # 7 days
            | ConcurrencyRules.Orphaned()
        ),
        maximum_to_prune: int = 10000,
        poll_interval: int = 60,
    ):
        super().__init__()

        # Handle backward compatibility with deprecation warning
        if rule is not None and job_rule is not None:
            job_rule = None  # For backward compatibility

        if rule is not None:
            import warnings

            warnings.warn(
                "The 'rule' parameter is deprecated and will be removed in a future version. "
                "Use 'job_rule' instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            job_rule = rule

        self.job_rule = job_rule
        self.concurrency_rule = concurrency_rule
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
            async with chancy.pool.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cursor:
                    with timed_block() as chancy_time:
                        # Prune jobs
                        job_rows_removed = await self.prune_jobs(chancy, cursor)

                        # Prune concurrency configs
                        concurrency_rule_rows_removed = (
                            await self.prune_concurrency_rules(chancy, cursor)
                        )

                        chancy.log.info(
                            f"Pruner removed {job_rows_removed} job(s) and "
                            f"{concurrency_rule_rows_removed} concurrency config(s). "
                            f"Took {chancy_time.elapsed:.2f} seconds."
                        )

                        await chancy.notify(
                            cursor,
                            "pruner.removed",
                            {
                                "elapsed": chancy_time.elapsed,
                                "job_rows_removed": job_rows_removed,
                                "concurrency_rule_rows_removed": concurrency_rule_rows_removed,
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

    async def prune_jobs(
        self, chancy: Chancy, cursor: AsyncCursor[DictRow]
    ) -> int:
        """
        Prune stale records from the database.

        :param chancy: The Chancy application.
        :param cursor: The database cursor to use for the operation.
        :return: The number of rows removed from the database
        """
        if self.job_rule is None:
            return 0

        job_query = sql.SQL(
            """
            WITH jobs_to_prune AS (
                SELECT queue, id  
                FROM {table}
                WHERE state NOT IN ('pending', 'running')
                AND ({rule})
                LIMIT {maximum_to_prune}
            )
            DELETE FROM {table} t
            USING jobs_to_prune p 
            WHERE t.queue = p.queue AND t.id = p.id
            """
        ).format(
            table=sql.Identifier(f"{chancy.prefix}jobs"),
            rule=self.job_rule.to_sql(),
            maximum_to_prune=sql.Literal(self.maximum_to_prune),
        )

        await cursor.execute(job_query)
        return cursor.rowcount

    async def prune_concurrency_rules(
        self, chancy: "Chancy", cursor: AsyncCursor[DictRow]
    ) -> int:
        """
        Prune stale concurrency rule records from the database.

        :param chancy: The Chancy application.
        :param cursor: The database cursor to use for the operation.
        :return: The number of rows removed from the database
        """
        if self.concurrency_rule is None:
            return 0

        rule_sql = self.concurrency_rule.to_sql(
            {"chancy_prefix": chancy.prefix}
        )

        config_query = sql.SQL(
            """
            DELETE FROM {concurrency_configs}
            WHERE ({rule})
            """
        ).format(
            concurrency_configs=sql.Identifier(
                f"{chancy.prefix}concurrency_configs"
            ),
            rule=rule_sql,
        )

        await cursor.execute(config_query)
        return cursor.rowcount
