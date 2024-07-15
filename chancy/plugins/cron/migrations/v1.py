from psycopg import AsyncCursor
from psycopg import sql

from chancy.migrate import Migration, Migrator


class V1Migration(Migration):
    async def up(self, migrator: Migrator, cursor: AsyncCursor):
        await cursor.execute(
            sql.SQL(
                """
                CREATE TABLE {table} (
                    unique_key TEXT PRIMARY KEY,
                    queue TEXT NOT NULL,
                    job JSON NOT NULL,
                    cron TEXT NOT NULL,
                    last_run TIMESTAMPTZ,
                    next_run TIMESTAMPTZ NOT NULL
                )
                """
            ).format(table=sql.Identifier(f"{migrator.prefix}cron"))
        )

    async def down(self, migrator: Migrator, cursor: AsyncCursor):
        await cursor.execute(
            sql.SQL("DROP TABLE {table}").format(
                table=sql.Identifier(f"{migrator.prefix}cron")
            )
        )
