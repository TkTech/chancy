from psycopg import AsyncCursor, sql
from psycopg.rows import DictRow

from chancy.migrate import Migration, Migrator


class V1Migration(Migration):
    async def up(self, migrator: Migrator, cursor: AsyncCursor[DictRow]):
        await cursor.execute(
            sql.SQL(
                """
                CREATE TABLE {table} (
                    unique_key TEXT PRIMARY KEY,
                    job JSON NOT NULL,
                    cron TEXT NOT NULL,
                    last_run TIMESTAMPTZ,
                    next_run TIMESTAMPTZ NOT NULL
                )
                """
            ).format(table=sql.Identifier(f"{migrator.prefix}cron"))
        )

    async def down(self, migrator: Migrator, cursor: AsyncCursor[DictRow]):
        await cursor.execute(
            sql.SQL("DROP TABLE {table}").format(
                table=sql.Identifier(f"{migrator.prefix}cron")
            )
        )
