from psycopg import AsyncConnection
from psycopg import sql

from chancy.app import Chancy
from chancy.migrate import Migration


class V1Migration(Migration):
    async def up(self, app: Chancy, conn: AsyncConnection):
        """
        Create the initial $prefix_jobs, $prefix_leaders tables.
        """
        async with conn.transaction():
            await conn.execute(
                sql.SQL(
                    """
                    CREATE TABLE {jobs} (
                        id SERIAL PRIMARY KEY,
                        queue TEXT NOT NULL,
                        payload JSONB NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        started_at TIMESTAMPTZ,
                        completed_at TIMESTAMPTZ
                    )
                    """
                ).format(jobs=sql.Identifier(f"{app.prefix}jobs"))
            )

            await conn.execute(
                sql.SQL(
                    """
                    CREATE TABLE {leaders} (
                        id SERIAL PRIMARY KEY,
                        worker_id TEXT NOT NULL,
                        last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                ).format(leaders=sql.Identifier(f"{app.prefix}leaders"))
            )

    async def down(self, app: Chancy, conn: AsyncConnection):
        """
        Drop the $prefix_jobs, $prefix_leaders tables.
        """
        async with conn.transaction():
            await conn.execute(
                sql.SQL("DROP TABLE {jobs}").format(
                    jobs=sql.Identifier(f"{app.prefix}jobs")
                )
            )
            await conn.execute(
                sql.SQL("DROP TABLE {leaders}").format(
                    leaders=sql.Identifier(f"{app.prefix}leaders")
                )
            )
