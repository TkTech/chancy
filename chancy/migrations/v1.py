from psycopg import AsyncConnection
from psycopg import sql

from chancy.migrate import Migration, Migrator


class V1Migration(Migration):
    async def up(self, migrator: Migrator, conn: AsyncConnection):
        """
        Create the initial $prefix_jobs, $prefix_leaders tables.
        """
        async with conn.transaction():
            await conn.execute(
                sql.SQL(
                    """
                    CREATE TABLE {jobs} (
                        id BIGSERIAL PRIMARY KEY,
                        queue TEXT NOT NULL,
                        payload JSON NOT NULL,
                        state VARCHAR (25) NOT NULL DEFAULT 'pending',
                        priority INTEGER DEFAULT 0,
                        attempts INTEGER DEFAULT 0,
                        max_attempts INTEGER DEFAULT 20,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        started_at TIMESTAMPTZ,
                        completed_at TIMESTAMPTZ
                    )
                    """
                ).format(jobs=sql.Identifier(f"{migrator.prefix}jobs"))
            )

            await conn.execute(
                sql.SQL(
                    """
                    CREATE TABLE {leaders} (
                        id BIGSERIAL PRIMARY KEY,
                        worker_id TEXT NOT NULL,
                        last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                ).format(leaders=sql.Identifier(f"{migrator.prefix}leaders"))
            )

    async def down(self, migrator: Migrator, conn: AsyncConnection):
        """
        Drop the $prefix_jobs, $prefix_leaders tables.
        """
        async with conn.transaction():
            await conn.execute(
                sql.SQL("DROP TABLE {jobs}").format(
                    jobs=sql.Identifier(f"{migrator.prefix}jobs")
                )
            )
            await conn.execute(
                sql.SQL("DROP TABLE {leaders}").format(
                    leaders=sql.Identifier(f"{migrator.prefix}leaders")
                )
            )
