from psycopg import sql
from psycopg.rows import dict_row

from chancy.plugin import Plugin
from chancy.rule import JobRules
from chancy.session import (
    Authority,
    OperationContext,
    PluginSession,
    TransactionContext,
)


class Reprioritize(Plugin):
    """
    A plugin that increases the priority of jobs based on how long they've been
    in the queue. This helps prevent job starvation by gradually increasing the
    priority of older jobs.

    .. code-block:: python

        async with Chancy(..., plugins=[
            Reprioritize(
                rule=(
                    Reprioritize.Rules.Age() > 600) &
                    (Reprioritize.Rules.Queue() == "high-priority")
                ),
                check_interval=300,
                priority_increase=1,
        ]) as chancy:
            ...

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
        if batch_size < 1:
            raise ValueError("batch_size must be greater than 0")
        self.batch_size = batch_size

    @staticmethod
    def get_identifier() -> str:
        return "chancy.reprioritize"

    @staticmethod
    def get_dependencies() -> list[str]:
        return ["chancy.leadership"]

    async def setup(self, session: PluginSession):
        session.periodic(
            "reprioritize",
            self._reprioritize,
            interval=self.check_interval,
            authority=Authority.LEADER,
        )

    async def _reprioritize(self, ctx: OperationContext):
        updated = await self.reprioritize_jobs(ctx.chancy, context=ctx)
        ctx.log.info("Reprioritized %s jobs.", updated)

    async def reprioritize_jobs(self, chancy, *, context=None) -> int:
        """
        Increase matching priorities in batches, once per invocation.

        :param chancy: The Chancy application.
        :param context: Optional operation context for transaction retries.
        :return: The number of jobs updated.
        """
        table = sql.Identifier(f"{chancy.prefix}jobs")

        async def execute(callback):
            if context is not None:
                return await context.database(callback)
            async with (
                chancy.pool.connection() as conn,
                conn.transaction(),
                conn.cursor(row_factory=dict_row) as cursor,
            ):
                return await callback(TransactionContext(cursor, chancy))

        async def boundary(tx):
            await tx.cursor.execute(
                sql.SQL("SELECT id FROM {} ORDER BY id DESC LIMIT 1").format(
                    table
                )
            )
            return await tx.cursor.fetchone()

        upper = await execute(boundary)
        if upper is None:
            return 0
        last_id = None
        total = 0
        while True:
            targets = None

            async def batch(tx, last_id=last_id):
                nonlocal targets
                cursor = tx.cursor
                if targets is None:
                    await cursor.execute(
                        sql.SQL(
                            """
                            SELECT id, priority FROM {table}
                            WHERE state IN ('pending', 'retrying')
                              AND ({rule}) AND id <= {upper}
                              AND ({last}::uuid IS NULL OR id > {last})
                            ORDER BY id LIMIT {limit}
                            FOR UPDATE SKIP LOCKED
                            """
                        ).format(
                            table=table,
                            rule=self.rule.to_sql(),
                            limit=sql.Literal(self.batch_size),
                            upper=sql.Literal(upper["id"]),
                            last=sql.Literal(last_id),
                        ),
                    )
                    # Retain absolute targets across an uncertain commit.
                    targets = [
                        (row["id"], row["priority"] + self.priority_increase)
                        for row in await cursor.fetchall()
                    ]
                if not targets:
                    return 0
                await cursor.execute(
                    sql.SQL(
                        """
                        UPDATE {table} j
                        SET priority = {choose}(j.priority, target.priority)
                        FROM unnest(%s::uuid[], %s::integer[]) AS target(id, priority)
                        WHERE j.id = target.id
                          AND j.state IN ('pending', 'retrying')
                        """
                    ).format(
                        table=table,
                        choose=sql.SQL(
                            "GREATEST"
                            if self.priority_increase >= 0
                            else "LEAST"
                        ),
                    ),
                    [[row[0] for row in targets], [row[1] for row in targets]],
                )
                return cursor.rowcount

            total += await execute(batch)
            if not targets:
                return total
            last_id = targets[-1][0]
