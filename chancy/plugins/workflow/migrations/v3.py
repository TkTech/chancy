from psycopg import AsyncCursor, sql
from psycopg.rows import DictRow

from chancy.migrate import Migration, Migrator


class ActiveWorkflowIndex(Migration):
    """Keep workflow polling independent of retained terminal history."""

    async def up(self, migrator: Migrator, cursor: AsyncCursor[DictRow]):
        await cursor.execute(
            sql.SQL(
                """
                CREATE INDEX {index} ON {workflows} (id)
                WHERE state IN ('pending', 'running')
                """
            ).format(
                index=sql.Identifier(
                    f"{migrator.prefix}workflows_active_id_idx"
                ),
                workflows=sql.Identifier(f"{migrator.prefix}workflows"),
            )
        )

    async def down(self, migrator: Migrator, cursor: AsyncCursor[DictRow]):
        await cursor.execute(
            sql.SQL("DROP INDEX {index}").format(
                index=sql.Identifier(
                    f"{migrator.prefix}workflows_active_id_idx"
                )
            )
        )
