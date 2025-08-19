from psycopg import sql

from chancy.migrate import Migration


class AddConcurrencySupport(Migration):
    """
    Add support for job-level concurrency constraints.

    This migration adds:
    1. concurrency_key column to jobs table for storing computed concurrency keys (prefixed with func_name)
    2. concurrency_configs table using prefixed concurrency_key as primary key
    3. Optimized indexes for concurrency-aware job selection
    """

    async def up(self, migrator, cursor):
        # Add concurrency_key column to jobs table
        await cursor.execute(
            sql.SQL(
                """
                ALTER TABLE {jobs}
                ADD COLUMN concurrency_key TEXT
                """
            ).format(jobs=sql.Identifier(f"{migrator.prefix}jobs"))
        )

        # Create concurrency configurations table
        await cursor.execute(
            sql.SQL(
                """
                CREATE TABLE {concurrency_configs} (
                    concurrency_key TEXT PRIMARY KEY,
                    concurrency_max INTEGER NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
                """
            ).format(
                concurrency_configs=sql.Identifier(
                    f"{migrator.prefix}concurrency_configs"
                )
            )
        )

    async def down(self, migrator, cursor):
        # Drop concurrency configurations table
        await cursor.execute(
            sql.SQL("DROP TABLE IF EXISTS {concurrency_configs}").format(
                concurrency_configs=sql.Identifier(
                    f"{migrator.prefix}concurrency_configs"
                )
            )
        )

        # Remove concurrency_key column from jobs table
        await cursor.execute(
            sql.SQL("DROP TABLE IF EXISTS {concurrency_configs}").format(
                concurrency_configs=sql.Identifier(
                    f"{migrator.prefix}concurrency_configs"
                )
            )
        )

        # Remove concurrency_key column from jobs table
        await cursor.execute(
            sql.SQL(
                """
                ALTER TABLE {jobs}
                DROP COLUMN IF EXISTS concurrency_key
                """
            ).format(jobs=sql.Identifier(f"{migrator.prefix}jobs"))
        )
