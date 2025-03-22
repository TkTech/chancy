from psycopg import AsyncCursor, sql
from psycopg.rows import DictRow

from chancy.migrate import Migration, Migrator


class ConvertToJSONB(Migration):
    """
    Convert JSON fields to JSONB in workflow tables for better performance and indexing capabilities.

    This migration updates all JSON fields in workflow tables to use JSONB type, which provides:
    - Better performance for reads
    - Ability to index JSON fields
    - More efficient storage
    """

    async def up(self, migrator: Migrator, cursor: AsyncCursor[DictRow]):
        # Update workflow_steps table
        await cursor.execute(
            sql.SQL(
                """
                ALTER TABLE {workflow_steps}
                ALTER COLUMN job_data TYPE JSONB USING job_data::JSONB,
                ALTER COLUMN dependencies TYPE JSONB USING dependencies::JSONB
                """
            ).format(
                workflow_steps=sql.Identifier(
                    f"{migrator.prefix}workflow_steps"
                )
            )
        )

    async def down(self, migrator: Migrator, cursor: AsyncCursor[DictRow]):
        # Revert workflow_steps table
        await cursor.execute(
            sql.SQL(
                """
                ALTER TABLE {workflow_steps}
                ALTER COLUMN job_data TYPE JSON USING job_data::JSON,
                ALTER COLUMN dependencies TYPE JSON USING dependencies::JSON
                """
            ).format(
                workflow_steps=sql.Identifier(
                    f"{migrator.prefix}workflow_steps"
                )
            )
        )
