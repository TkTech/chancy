from psycopg import sql

from chancy.migrate import Migration


class MetricsBuckets(Migration):
    """Replace ambiguous retained arrays with independently expiring buckets."""

    async def up(self, migrator, cursor):
        metrics = sql.Identifier(f"{migrator.prefix}metrics")
        definitions = sql.Identifier(f"{migrator.prefix}metric_definitions")
        await cursor.execute(
            sql.SQL("DROP TABLE {metrics}").format(metrics=metrics)
        )
        await cursor.execute(
            sql.SQL(
                """
                CREATE TABLE {definitions} (
                    id BIGSERIAL PRIMARY KEY,
                    key TEXT COLLATE "C" NOT NULL UNIQUE,
                    metric_type TEXT NOT NULL CHECK (
                        metric_type IN ('counter', 'gauge', 'histogram')
                    ),
                    unit TEXT NOT NULL
                );

                CREATE TABLE {metrics} (
                    metric_id BIGINT NOT NULL
                        REFERENCES {definitions}(id) ON DELETE RESTRICT,
                    resolution INTEGER NOT NULL,
                    chunk TIMESTAMPTZ NOT NULL,
                    session_id UUID NOT NULL,
                    worker_id TEXT NOT NULL,
                    buckets TIMESTAMPTZ[] NOT NULL,
                    counts BIGINT[],
                    totals DOUBLE PRECISION[],
                    minimums DOUBLE PRECISION[],
                    maximums DOUBLE PRECISION[],
                    gauges DOUBLE PRECISION[],
                    sampled_at TIMESTAMPTZ[] NOT NULL,
                    PRIMARY KEY (metric_id, resolution, chunk, session_id)
                );

                CREATE INDEX {expiry_index} ON {metrics} (resolution, chunk);
                """
            ).format(
                metrics=metrics,
                definitions=definitions,
                expiry_index=sql.Identifier(
                    f"{migrator.prefix}metrics_expiry_idx"
                ),
            )
        )

    async def down(self, migrator, cursor):
        from . import v1

        await cursor.execute(
            sql.SQL(
                """
                DROP TABLE {metrics};
                DROP TABLE {definitions}
                """
            ).format(
                metrics=sql.Identifier(f"{migrator.prefix}metrics"),
                definitions=sql.Identifier(
                    f"{migrator.prefix}metric_definitions"
                ),
            )
        )
        await v1.MetricsInitialMigration().up(migrator, cursor)
