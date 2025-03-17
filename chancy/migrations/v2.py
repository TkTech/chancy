from psycopg import AsyncCursor, sql
from psycopg.rows import DictRow

from chancy.migrate import Migration, Migrator


class V2Migration(Migration):
    async def up(self, migrator: Migrator, cursor: AsyncCursor[DictRow]):
        """
        Adds common indexes.
        """
        await cursor.execute(
            sql.SQL(
                """
                CREATE INDEX IF NOT EXISTS {idx_queue_state}
                ON {jobs} (queue, state);
                """
            ).format(
                jobs=sql.Identifier(f"{migrator.prefix}jobs"),
                idx_queue_state=sql.Identifier(
                    f"{migrator.prefix}idx_jobs_queue_state"
                ),
            )
        )

        await cursor.execute(
            sql.SQL(
                """
                CREATE INDEX IF NOT EXISTS {idx_jobs_fetch_prune} ON {jobs}
                (queue, state, attempts, scheduled_at, priority DESC, id DESC)
                WHERE state IN ('pending', 'retrying');
                """
            ).format(
                jobs=sql.Identifier(f"{migrator.prefix}jobs"),
                idx_jobs_fetch_prune=sql.Identifier(
                    f"{migrator.prefix}idx_jobs_fetch_prune"
                ),
            )
        )

    async def down(self, migrator: Migrator, cursor: AsyncCursor[DictRow]):
        await cursor.execute(
            sql.SQL(
                """
                DROP INDEX IF EXISTS {idx_queue_state};
                """
            ).format(
                idx_queue_state=sql.Identifier(
                    f"{migrator.prefix}idx_jobs_queue_state"
                )
            )
        )

        await cursor.execute(
            sql.SQL(
                """
                DROP INDEX IF EXISTS {idx_jobs_fetch_prune};
                """
            ).format(
                idx_jobs_fetch_prune=sql.Identifier(
                    f"{migrator.prefix}idx_jobs_fetch_prune"
                )
            )
        )
