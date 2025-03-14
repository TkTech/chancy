"""
Add metric_type column to metrics table.
"""

from psycopg import AsyncCursor, sql

from chancy.migrate import Migration, Migrator


class MetricsAddTypeColumn(Migration):
    """
    Add metric_type column to metrics table.
    """

    async def up(self, migrator: Migrator, cursor: AsyncCursor):
        """
        Add the metric_type column.
        """
        # Erase all existing metrics
        await cursor.execute(
            sql.SQL(
                """
                TRUNCATE TABLE {metrics_table}
                """
            ).format(metrics_table=sql.Identifier(f"{migrator.prefix}metrics"))
        )

        await cursor.execute(
            sql.SQL(
                """
                ALTER TABLE {metrics_table}
                ADD COLUMN IF NOT EXISTS metric_type VARCHAR(20) NOT NULL
                """
            ).format(metrics_table=sql.Identifier(f"{migrator.prefix}metrics"))
        )

    async def down(self, migrator: Migrator, cursor: AsyncCursor):
        """
        Remove the metric_type column.
        """
        await cursor.execute(
            sql.SQL(
                """
                ALTER TABLE {metrics_table}
                DROP COLUMN IF EXISTS metric_type
                """
            ).format(metrics_table=sql.Identifier(f"{migrator.prefix}metrics"))
        )
