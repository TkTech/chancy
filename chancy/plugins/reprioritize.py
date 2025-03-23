from psycopg import sql
from psycopg.rows import dict_row

from chancy.plugin import Plugin
from chancy.rule import JobRules


class Reprioritize(Plugin):
    """
    A plugin that increases the priority of jobs based on how long they've been
    in the queue. This helps prevent job starvation by gradually increasing the
    priority of older jobs.

    Example usage:

    .. code-block:: python

        plugin = Reprioritize(
            rule=(
                (Reprioritize.Rules.Age() > 600) &
                (Reprioritize.Rules.Queue() == "high-priority")
            ),
            check_interval=300,
            priority_increase=1
        )

    This means that any job that has been in the queue for more than 10 minutes
    will have its priority increased by 1 every 5 minutes, but only for jobs
    in the high-priority queue.
    """

    Rules = JobRules

    def __init__(
        self,
        rule,
        *,
        check_interval: int = 300,
        priority_increase: int = 1,
        batch_size: int = 1000,
    ):
        super().__init__()
        self.rule = rule
        self.check_interval = check_interval
        self.priority_increase = priority_increase
        self.batch_size = batch_size

    @staticmethod
    def get_identifier() -> str:
        return "chancy.reprioritize"

    @staticmethod
    def get_dependencies() -> list[str]:
        return ["chancy.leadership"]

    async def run(self, worker, chancy):
        while await self.sleep(self.check_interval):
            await self.wait_for_leader(worker)
            updated = await self.reprioritize_jobs(chancy)
            chancy.log.info(
                f"Reprioritized {updated} jobs by increasing priority"
                f" by {self.priority_increase}"
            )

    async def reprioritize_jobs(self, chancy) -> int:
        """
        Reprioritize jobs based on the rules provided to the plugin.

        Returns the total number of jobs that were updated.
        """
        total_updated = 0

        async with chancy.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                while True:
                    # Update jobs in batches to avoid long-running transactions
                    async with conn.transaction():
                        await cursor.execute(
                            sql.SQL(
                                """
                                WITH jobs_to_update AS (
                                    SELECT 
                                        id,
                                        priority
                                    FROM {jobs_table}
                                    WHERE 
                                        state IN ('pending', 'retrying')
                                        AND ({rule})
                                    ORDER BY id
                                    LIMIT {batch_size}
                                    FOR UPDATE SKIP LOCKED
                                )
                                UPDATE {jobs_table} j
                                SET 
                                    priority = j.priority + {increment}
                                FROM jobs_to_update
                                WHERE j.id = jobs_to_update.id
                                RETURNING j.id
                                """
                            ).format(
                                jobs_table=sql.Identifier(
                                    f"{chancy.prefix}jobs"
                                ),
                                rule=self.rule.to_sql(),
                                increment=self.priority_increase,
                                batch_size=self.batch_size,
                            )
                        )

                        results = await cursor.fetchall()
                        if not results:
                            break

                        total_updated += len(results)

        return total_updated
