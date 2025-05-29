from psycopg import AsyncCursor, sql
from psycopg.rows import DictRow

from chancy.migrate import Migration, Migrator


class V1Migration(Migration):
    async def up(self, migrator: Migrator, cursor: AsyncCursor[DictRow]):
        """
        Create the triggers configuration table.
        """
        await cursor.execute(
            sql.SQL("""
                CREATE TABLE {table} (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    table_name TEXT NOT NULL,
                    schema_name TEXT NOT NULL DEFAULT 'public',
                    trigger_name TEXT NOT NULL,
                    operations JSONB NOT NULL,
                    job_template JSONB NOT NULL,
                    enabled BOOLEAN NOT NULL DEFAULT true,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE(schema_name, table_name, trigger_name)
                )
            """).format(table=sql.Identifier(f"{migrator.prefix}triggers"))
        )

        await cursor.execute(
            sql.SQL("""
                CREATE INDEX {index_name}
                ON {table} (schema_name, table_name, trigger_name)
                WHERE enabled = true
            """).format(
                index_name=sql.Identifier(
                    f"{migrator.prefix}triggers_lookup_idx"
                ),
                table=sql.Identifier(f"{migrator.prefix}triggers"),
            )
        )

    async def down(self, migrator: Migrator, cursor: AsyncCursor[DictRow]):
        """
        Drop the triggers table and clean up any remaining trigger functions.
        """
        await cursor.execute(
            sql.SQL("""
                DO $$
                DECLARE
                    trigger_rec RECORD;
                BEGIN
                    FOR trigger_rec IN 
                        SELECT 
                            tg.tgname AS triggername,
                            n.nspname AS schemaname,
                            c.relname AS tablename
                        FROM pg_catalog.pg_trigger tg
                        JOIN pg_catalog.pg_class c ON tg.tgrelid = c.oid
                        JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                        WHERE NOT tg.tgisinternal
                          AND tg.tgname LIKE {prefix}
                    LOOP
                        EXECUTE format(
                            'DROP TRIGGER IF EXISTS %I ON %I.%I',
                            trigger_rec.triggername, 
                            trigger_rec.schemaname, 
                            trigger_rec.tablename
                        );
                    END LOOP;
                END $$;
            """).format(prefix=sql.Literal(f"{migrator.prefix}trigger_%")),
        )

        await cursor.execute(
            sql.SQL("DROP FUNCTION IF EXISTS {function_name}()").format(
                function_name=sql.Identifier(
                    f"{migrator.prefix}trigger_handler"
                )
            )
        )

        await cursor.execute(
            sql.SQL("DROP TABLE IF EXISTS {table}").format(
                table=sql.Identifier(f"{migrator.prefix}triggers")
            )
        )
