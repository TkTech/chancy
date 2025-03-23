from psycopg import AsyncCursor, sql
from psycopg.rows import DictRow

from chancy.migrate import Migration, Migrator


class ConvertToJSONB(Migration):
    """
    Convert JSON fields to JSONB in cron table for better performance and indexing capabilities.

    This migration updates the job JSON field in cron table to use JSONB type, which provides:
    - Better performance for reads
    - Ability to index JSON fields
    - More efficient storage
    """

    async def up(self, migrator: Migrator, cursor: AsyncCursor[DictRow]):
        # Update cron table
        await cursor.execute(
            sql.SQL(
                """
                ALTER TABLE {cron}
                ALTER COLUMN job TYPE JSONB USING job::JSONB
                """
            ).format(cron=sql.Identifier(f"{migrator.prefix}cron"))
        )

    async def down(self, migrator: Migrator, cursor: AsyncCursor[DictRow]):
        # Revert cron table
        await cursor.execute(
            sql.SQL(
                """
                ALTER TABLE {cron}
                ALTER COLUMN job TYPE JSON USING job::JSON
                """
            ).format(cron=sql.Identifier(f"{migrator.prefix}cron"))
        )
