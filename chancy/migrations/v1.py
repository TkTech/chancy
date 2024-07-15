from psycopg import AsyncCursor
from psycopg import sql

from chancy.migrate import Migration, Migrator


class V1Migration(Migration):
    async def up(self, migrator: Migrator, cursor: AsyncCursor):
        """
        Create the initial $prefix_jobs, $prefix_leaders tables.
        """
        await cursor.execute(
            sql.SQL(
                """
                CREATE TABLE {jobs} (
                    id UUID PRIMARY KEY,
                    queue TEXT NOT NULL,
                    payload JSON NOT NULL,
                    state VARCHAR (25) NOT NULL DEFAULT 'pending',
                    priority INTEGER DEFAULT 10,
                    attempts INTEGER DEFAULT 0,
                    max_attempts INTEGER DEFAULT 1,
                    taken_by TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    started_at TIMESTAMPTZ,
                    completed_at TIMESTAMPTZ,
                    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    unique_key TEXT
                )
                """
            ).format(jobs=sql.Identifier(f"{migrator.prefix}jobs"))
        )
        await cursor.execute(
            sql.SQL(
                """
                CREATE UNIQUE INDEX {jobs_unique_key}
                ON {jobs} (unique_key)
                WHERE
                    unique_key IS NOT NULL
                        AND state NOT IN ('succeeded', 'failed');
                """
            ).format(
                jobs_unique_key=sql.Identifier(
                    f"{migrator.prefix}jobs_unique_key"
                ),
                jobs=sql.Identifier(f"{migrator.prefix}jobs"),
            )
        )
        await cursor.execute(
            sql.SQL(
                """
                CREATE UNLOGGED TABLE {leader} (
                    id BIGSERIAL PRIMARY KEY,
                    worker_id TEXT NOT NULL,
                    first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    expires_at TIMESTAMPTZ NOT NULL
                );
                ALTER TABLE {leader}
                    ADD CONSTRAINT leader_worker_id_unique UNIQUE
                        (worker_id);
                """
            ).format(leader=sql.Identifier(f"{migrator.prefix}leader"))
        )

        await cursor.execute(
            sql.SQL(
                """
                CREATE UNLOGGED TABLE {workers} (
                    worker_id TEXT NOT NULL PRIMARY KEY,
                    last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            ).format(workers=sql.Identifier(f"{migrator.prefix}workers"))
        )

    async def down(self, migrator: Migrator, cursor: AsyncCursor):
        """
        Drop the $prefix_jobs, $prefix_leaders tables.
        """
        await cursor.execute(
            sql.SQL("DROP TABLE {jobs}").format(
                jobs=sql.Identifier(f"{migrator.prefix}jobs")
            )
        )
        await cursor.execute(
            sql.SQL("DROP TABLE {leader}").format(
                leader=sql.Identifier(f"{migrator.prefix}leader")
            )
        )
        await cursor.execute(
            sql.SQL("DROP TABLE {workers}").format(
                workers=sql.Identifier(f"{migrator.prefix}workers")
            )
        )
