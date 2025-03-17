from psycopg import AsyncCursor, sql
from psycopg.rows import DictRow

from chancy.migrate import Migration, Migrator


class V1Migration(Migration):
    async def up(self, migrator: Migrator, cursor: AsyncCursor[DictRow]):
        """
        Create the initial $prefix_jobs, $prefix_leaders tables.
        """
        await cursor.execute(
            sql.SQL(
                """
                CREATE TABLE {jobs} (
                    id UUID PRIMARY KEY,
                    queue TEXT NOT NULL,
                    func TEXT NOT NULL,
                    kwargs JSON NOT NULL DEFAULT '{{}}',
                    limits JSON NOT NULL DEFAULT '[]',
                    meta JSON NOT NULL DEFAULT '{{}}',
                    state VARCHAR (25) NOT NULL DEFAULT 'pending',
                    priority INTEGER DEFAULT 0,
                    attempts INTEGER DEFAULT 0,
                    max_attempts INTEGER DEFAULT 1,
                    taken_by TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    started_at TIMESTAMPTZ,
                    completed_at TIMESTAMPTZ,
                    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    unique_key TEXT,
                    errors JSON NOT NULL DEFAULT '[]'
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
                    -- The UUID of the worker.
                    worker_id TEXT NOT NULL PRIMARY KEY,
                    -- The tags associated with this worker.
                    tags TEXT[],
                    -- The queues this worker is currently processing.
                    queues TEXT[],
                    last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    expires_at TIMESTAMPTZ NOT NULL
                );
                """
            ).format(workers=sql.Identifier(f"{migrator.prefix}workers"))
        )

        await cursor.execute(
            sql.SQL(
                """
                CREATE UNLOGGED TABLE {rate_limit} (
                    queue VARCHAR(255) PRIMARY KEY,
                    window_start INTEGER NOT NULL,
                    count INTEGER NOT NULL DEFAULT 0
                );
                """
            ).format(
                rate_limit=sql.Identifier(f"{migrator.prefix}queue_rate_limits")
            )
        )

        await cursor.execute(
            sql.SQL(
                """
                CREATE TABLE {queues} (
                    -- A unique name for the queue.
                    name TEXT PRIMARY KEY,
                    -- The state of the queue. This can be used to pause
                    -- processing of jobs in the queue.
                    state TEXT NOT NULL DEFAULT 'active',
                    -- The number of jobs that can be processed concurrently
                    -- per worker.
                    concurrency INTEGER DEFAULT 1,
                    -- Only worker's matching these tags will be able to
                    -- process jobs from this queue, or None for all workers.
                    tags TEXT[],
                    -- The executor to use for processing jobs in this queue.
                    executor TEXT NOT NULL DEFAULT
                        'chancy.executors.process.ProcessExecutor',
                    -- Options to pass to the executor.
                    executor_options JSON,
                    -- The interval at which the queue should be polled for
                    -- jobs.
                    polling_interval INTEGER DEFAULT 1,
                    -- The time at which the queue was first declared.
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    -- The number of jobs that can be processed per period.
                    rate_limit INTEGER,
                    -- The period of time over which the rate limit applies.
                    rate_limit_window INTEGER
                );
                """
            ).format(queues=sql.Identifier(f"{migrator.prefix}queues"))
        )

    async def down(self, migrator: Migrator, cursor: AsyncCursor[DictRow]):
        """
        Drop the $prefix_jobs, $prefix_leaders tables.
        """
        await cursor.execute(
            sql.SQL("DROP TABLE IF EXISTS {jobs}").format(
                jobs=sql.Identifier(f"{migrator.prefix}jobs")
            )
        )
        await cursor.execute(
            sql.SQL("DROP TABLE IF EXISTS {leader}").format(
                leader=sql.Identifier(f"{migrator.prefix}leader")
            )
        )
        await cursor.execute(
            sql.SQL("DROP TABLE IF EXISTS {workers}").format(
                workers=sql.Identifier(f"{migrator.prefix}workers")
            )
        )
        await cursor.execute(
            sql.SQL("DROP TABLE IF EXISTS {queues}").format(
                queues=sql.Identifier(f"{migrator.prefix}queues")
            )
        )
        await cursor.execute(
            sql.SQL("DROP TABLE IF EXISTS {rate_limit}").format(
                rate_limit=sql.Identifier(f"{migrator.prefix}queue_rate_limits")
            )
        )
