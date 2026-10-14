from psycopg import AsyncCursor, sql
from psycopg.rows import DictRow

from chancy.migrate import Migration, Migrator


class AddTimezone(Migration):
    """
    Add an IANA timezone to each cron schedule.

    Existing schedules get UTC, which keeps the historical behaviour of
    evaluating every cron expression in UTC.
    """

    async def up(self, migrator: Migrator, cursor: AsyncCursor[DictRow]):
        await cursor.execute(
            sql.SQL(
                """
                ALTER TABLE {cron}
                ADD COLUMN timezone TEXT NOT NULL DEFAULT 'Etc/UTC'
                """
            ).format(cron=sql.Identifier(f"{migrator.prefix}cron"))
        )

    async def down(self, migrator: Migrator, cursor: AsyncCursor[DictRow]):
        await cursor.execute(
            sql.SQL(
                """
                ALTER TABLE {cron}
                DROP COLUMN timezone
                """
            ).format(cron=sql.Identifier(f"{migrator.prefix}cron"))
        )
