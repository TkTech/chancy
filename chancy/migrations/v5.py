from psycopg import sql

from chancy.migrate import Migration


class ConvertToJSONB(Migration):
    """
    Convert JSON fields to JSONB for better performance and indexing capabilities.

    This migration updates all JSON fields in core tables to use JSONB type, which provides:
    - Better performance for reads
    - Ability to index JSON fields
    - More efficient storage
    """

    async def up(self, migrator, cursor):
        # Update the jobs table
        await cursor.execute(
            sql.SQL(
                """
                ALTER TABLE {jobs}
                ALTER COLUMN kwargs TYPE JSONB USING kwargs::JSONB,
                ALTER COLUMN limits TYPE JSONB USING limits::JSONB,
                ALTER COLUMN meta TYPE JSONB USING meta::JSONB,
                ALTER COLUMN errors TYPE JSONB USING errors::JSONB
                """
            ).format(jobs=sql.Identifier(f"{migrator.prefix}jobs"))
        )

        # Update the queues table
        await cursor.execute(
            sql.SQL(
                """
                ALTER TABLE {queues} 
                ALTER COLUMN executor_options TYPE JSONB USING 
                CASE WHEN executor_options IS NULL THEN NULL 
                ELSE executor_options::JSONB END
                """
            ).format(queues=sql.Identifier(f"{migrator.prefix}queues"))
        )

    async def down(self, migrator, cursor):
        # Revert the jobs table
        await cursor.execute(
            sql.SQL(
                """
                ALTER TABLE {jobs}
                ALTER COLUMN kwargs TYPE JSON USING kwargs::JSON,
                ALTER COLUMN limits TYPE JSON USING limits::JSON,
                ALTER COLUMN meta TYPE JSON USING meta::JSON,
                ALTER COLUMN errors TYPE JSON USING errors::JSON
                """
            ).format(jobs=sql.Identifier(f"{migrator.prefix}jobs"))
        )

        # Revert the queues table
        await cursor.execute(
            sql.SQL(
                """
                ALTER TABLE {queues} 
                ALTER COLUMN executor_options TYPE JSON USING 
                CASE WHEN executor_options IS NULL THEN NULL 
                ELSE executor_options::JSON END
                """
            ).format(queues=sql.Identifier(f"{migrator.prefix}queues"))
        )
