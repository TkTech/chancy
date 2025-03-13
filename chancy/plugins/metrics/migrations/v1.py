"""
Initial migration for the metrics plugin.
"""

from psycopg import AsyncCursor, sql

from chancy.migrate import Migration, Migrator


class MetricsInitialMigration(Migration):
    """
    Create the initial tables for the metrics plugin.
    """

    async def up(self, migrator: Migrator, cursor: AsyncCursor):
        """
        Create the metrics table.
        """
        await cursor.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {metrics_table} (
                    -- Primary key: metric_key + resolution + worker_id combination
                    metric_key VARCHAR(255) NOT NULL,
                    resolution VARCHAR(10) NOT NULL,  -- '1min', '5min', '1hour', etc.
                    worker_id VARCHAR(255) NOT NULL,  -- Worker identifier
                    
                    -- Storage for time-series data
                    -- timestamp -> value mapping, stored in descending order (newest first)
                    timestamps TIMESTAMPTZ[] NOT NULL,
                    values JSONB[] NOT NULL,
                    
                    -- Metadata
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    metadata JSONB NOT NULL DEFAULT '{{}}',
                    
                    -- Primary key
                    PRIMARY KEY (metric_key, resolution, worker_id)
                )
                """
            ).format(metrics_table=sql.Identifier(f"{migrator.prefix}metrics"))
        )

        # Create index on updated_at for syncing between workers
        await cursor.execute(
            sql.SQL(
                """
                CREATE INDEX IF NOT EXISTS {metrics_updated_idx} 
                ON {metrics_table} (updated_at)
                """
            ).format(
                metrics_table=sql.Identifier(f"{migrator.prefix}metrics"),
                metrics_updated_idx=sql.Identifier(
                    f"{migrator.prefix}metrics_updated_idx"
                ),
            )
        )

    async def down(self, migrator: Migrator, cursor: AsyncCursor):
        """
        Drop the metrics table.
        """
        await cursor.execute(
            sql.SQL("DROP TABLE IF EXISTS {metrics_table}").format(
                metrics_table=sql.Identifier(f"{migrator.prefix}metrics")
            )
        )
