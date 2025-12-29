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

        # Create partial index for efficient concurrency lookups on running jobs
        await cursor.execute(
            sql.SQL(
                """
                CREATE INDEX {index_name} ON {jobs} (concurrency_key)
                WHERE state = 'running' AND concurrency_key IS NOT NULL
                """
            ).format(
                index_name=sql.Identifier(
                    f"{migrator.prefix}jobs_concurrency_key_running_idx"
                ),
                jobs=sql.Identifier(f"{migrator.prefix}jobs"),
            )
        )

    async def down(self, migrator, cursor):
        # Drop the concurrency index
        await cursor.execute(
            sql.SQL("DROP INDEX IF EXISTS {index_name}").format(
                index_name=sql.Identifier(
                    f"{migrator.prefix}jobs_concurrency_key_running_idx"
                )
            )
        )

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
            sql.SQL(
                """
                ALTER TABLE {jobs}
                DROP COLUMN IF EXISTS concurrency_key
                """
            ).format(jobs=sql.Identifier(f"{migrator.prefix}jobs"))
        )
