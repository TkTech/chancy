from psycopg import AsyncCursor
from psycopg import sql
from psycopg_pool import AsyncConnectionPool

from chancy.app import Chancy


class Client:
    def __init__(
        self, app: Chancy, *, min_pool_size: int = 0, max_pool_size: int = 1
    ):
        self.app = app
        self.pool = AsyncConnectionPool(
            self.app.dsn,
            open=False,
            check=AsyncConnectionPool.check_connection,
            min_size=min_pool_size,
            max_size=max_pool_size,
        )
        self.jobs_table = sql.Identifier(f"{self.app.prefix}jobs")

    async def add_job_to_cursor(self, cur: AsyncCursor):
        await cur.execute(
            sql.SQL(
                """
                INSERT INTO {jobs}
                """,
            ).format(jobs=self.jobs_table)
        )

    async def __aenter__(self):
        await self.pool.open()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.pool.close()
        return False
