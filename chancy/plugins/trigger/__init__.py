import json
import uuid

from psycopg import sql
from psycopg.rows import dict_row

from chancy.plugin import Plugin
from chancy.app import Chancy
from chancy.job import Job, IsAJob


class Trigger(Plugin):
    """
    Install database triggers on non-Chancy tables that create jobs when rows
    change.

    The Trigger plugin allows you to automatically create jobs in response to
    database changes on any table. It uses PostgreSQL statement-level triggers
    for optimal performance with bulk operations. Since these triggers are run
    by the database itself, jobs will always be created - even if all Chancy
    workers are offline.

    .. tip::

        Database triggers should be avoided for extremely high-frequency
        operations or tables with very high write volumes, as they can introduce
        significant overhead.

    Usage
    -----

    You can register a job to run when a specific table is modified by
    creating a trigger. The trigger will listen for specified operations
    (INSERT, UPDATE, DELETE) and create a job using the provided job template:

    .. code-block:: python

        import asyncio
        from chancy import Chancy, Worker, Queue, job
        from chancy.plugins.trigger import Trigger

        @job(queue="user_events")
        def process_user_change():
            pass

        async with Chancy(
            "postgresql://localhost/postgres",
            plugins=[Trigger()]
        ) as chancy:
            await Trigger.register_trigger(
                chancy,
                table_name="users",
                job_template=process_user_change,
                operations=["INSERT", "UPDATE"]
            )

    More information about what triggered the job can be found in the job's
    metadata:

    .. code-block:: python

        from chancy import job, QueuedJob

        @job(queue="user_events")
        def process_user_change(*, context: QueuedJob):
            print(context.meta["trigger"])

    Would output something like:

    .. code-block::

        {
            'operation': 'INSERT',
            'timestamp': '2025-05-29T05:51:26.777476+00:00',
            'table_name': 'test_users',
            'schema_name': 'public',
            'trigger_name': 'chancy_trigger_675ed409_4da2_4967_bfc7_7bc641f1cc92_insert'
        }
    """

    def migrate_key(self) -> str | None:
        return "trigger"

    def migrate_package(self) -> str | None:
        return "chancy.plugins.trigger.migrations"

    def get_tables(self) -> list[str]:
        return ["triggers"]

    @staticmethod
    def get_identifier() -> str:
        return "chancy.trigger"

    @classmethod
    async def register_trigger(
        cls,
        chancy: Chancy,
        table_name: str,
        job_template: Job | IsAJob,
        *,
        operations: list[str] = None,
        schema_name: str = "public",
        enabled: bool = True,
    ) -> str:
        """
        Register a database trigger that creates jobs when table rows change.

        :param chancy: The Chancy application.
        :param table_name: Name of the table to monitor.
        :param job_template: Job template to create when trigger fires.
        :param operations: List of operations to monitor ('INSERT', 'UPDATE', 'DELETE').
        :param schema_name: Schema containing the table (default: 'public').
        :param enabled: Whether the trigger should be enabled immediately.
        :return: The unique trigger identifier.
        """
        if operations is None:
            operations = ["INSERT", "UPDATE", "DELETE"]

        valid_ops = {"INSERT", "UPDATE", "DELETE"}
        for op in operations:
            if op.upper() not in valid_ops:
                raise ValueError(
                    f"Invalid operation: {op}. Must be one of {valid_ops}"
                )

        operations = [op.upper() for op in operations]
        job = (
            job_template if isinstance(job_template, Job) else job_template.job
        )

        trigger_id = str(uuid.uuid4())
        trigger_name = f"{chancy.prefix}trigger_{trigger_id.replace('-', '_')}"

        async with chancy.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                async with conn.transaction():
                    await cls._ensure_trigger_function(cursor, chancy.prefix)
                    await cursor.execute(
                        sql.SQL("""
                            INSERT INTO {table} (
                                id,
                                table_name,
                                schema_name,
                                trigger_name,
                                operations,
                                job_template,
                                enabled
                            ) VALUES (
                                %(id)s,
                                %(table_name)s,
                                %(schema_name)s,
                                %(trigger_name)s,
                                %(operations)s,
                                %(job_template)s,
                                %(enabled)s
                            )
                        """).format(
                            table=sql.Identifier(f"{chancy.prefix}triggers")
                        ),
                        {
                            "id": trigger_id,
                            "table_name": table_name,
                            "schema_name": schema_name,
                            "trigger_name": trigger_name,
                            "operations": json.dumps(operations),
                            "job_template": json.dumps(job.pack()),
                            "enabled": enabled,
                        },
                    )

                    if enabled:
                        await cls._create_database_triggers(
                            cursor,
                            schema_name,
                            table_name,
                            trigger_name,
                            operations,
                            chancy.prefix,
                        )

        return trigger_id

    @classmethod
    async def unregister_trigger(cls, chancy: Chancy, trigger_id: str):
        """
        Permanently remove a trigger.

        :param chancy: The Chancy application.
        :param trigger_id: The unique trigger identifier to remove.
        """
        table = sql.Identifier(f"{chancy.prefix}triggers")

        async with chancy.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                async with conn.transaction():
                    # Get trigger info before deleting
                    await cursor.execute(
                        sql.SQL("""
                            SELECT
                                schema_name,
                                table_name,
                                trigger_name,
                                operations
                            FROM {table} WHERE id = %(id)s
                        """).format(table=table),
                        {"id": trigger_id},
                    )

                    row = await cursor.fetchone()
                    if not row:
                        raise ValueError(f"Trigger {trigger_id} not found")

                    for operation in row["operations"]:
                        trigger_name = (
                            f"{row['trigger_name']}_{operation.lower()}"
                        )
                        await cls._drop_database_trigger(
                            cursor,
                            row["schema_name"],
                            row["table_name"],
                            trigger_name,
                        )

                    # Remove from config table
                    await cursor.execute(
                        sql.SQL("DELETE FROM {table} WHERE id = %(id)s").format(
                            table=table
                        ),
                        {"id": trigger_id},
                    )

    @classmethod
    async def enable_trigger(cls, chancy: Chancy, trigger_id: str):
        """Enable a disabled trigger."""
        await cls._toggle_trigger(chancy, trigger_id, True)

    @classmethod
    async def disable_trigger(cls, chancy: Chancy, trigger_id: str):
        """Disable an enabled trigger."""
        await cls._toggle_trigger(chancy, trigger_id, False)

    @classmethod
    async def _toggle_trigger(
        cls, chancy: Chancy, trigger_id: str, enabled: bool
    ):
        """Toggle trigger enabled state."""
        table = sql.Identifier(f"{chancy.prefix}triggers")

        async with chancy.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                async with conn.transaction():
                    # Get current trigger info
                    await cursor.execute(
                        sql.SQL("""
                            SELECT
                                schema_name,
                                table_name,
                                trigger_name,
                                operations,
                                enabled
                            FROM {table} WHERE id = %(id)s
                        """).format(table=table),
                        {"id": trigger_id},
                    )

                    row = await cursor.fetchone()
                    if not row:
                        raise ValueError(f"Trigger {trigger_id} not found")

                    if row["enabled"] == enabled:
                        return

                    if enabled:
                        await cls._create_database_triggers(
                            cursor,
                            row["schema_name"],
                            row["table_name"],
                            row["trigger_name"],
                            row["operations"],
                            chancy.prefix,
                        )
                    else:
                        for operation in row["operations"]:
                            trigger_name = (
                                f"{row['trigger_name']}_{operation.lower()}"
                            )
                            await cls._drop_database_trigger(
                                cursor,
                                row["schema_name"],
                                row["table_name"],
                                trigger_name,
                            )

                    await cursor.execute(
                        sql.SQL("""
                            UPDATE
                                {table}
                            SET
                                enabled = %(enabled)s
                            WHERE id = %(id)s
                        """).format(table=table),
                        {"enabled": enabled, "id": trigger_id},
                    )

    @classmethod
    async def get_triggers(
        cls, chancy: Chancy, *, trigger_ids: list[str] = None
    ) -> dict[str, dict]:
        """
        Get registered triggers by their IDs.

        :param chancy: The Chancy application.
        :param trigger_ids: Optional list of trigger IDs to filter by.
        :return: Dictionary mapping trigger ID to trigger configuration.
        """
        table = sql.Identifier(f"{chancy.prefix}triggers")

        async with chancy.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    sql.SQL("""
                        SELECT 
                            id,
                            table_name,
                            schema_name,
                            trigger_name,
                            operations,
                            job_template,
                            enabled,
                            created_at
                        FROM {table}
                        WHERE (%(trigger_ids)s::text[] IS NULL OR id::text = ANY(%(trigger_ids)s))
                        ORDER BY created_at DESC
                    """).format(table=table),
                    {"trigger_ids": trigger_ids},
                )

                return {
                    row["id"]: {
                        "id": row["id"],
                        "table_name": row["table_name"],
                        "schema_name": row["schema_name"],
                        "trigger_name": row["trigger_name"],
                        "operations": row["operations"],
                        "job_template": Job.unpack(row["job_template"]),
                        "enabled": row["enabled"],
                        "created_at": row["created_at"],
                    }
                    async for row in cursor
                }

    @staticmethod
    async def _ensure_trigger_function(cursor, prefix: str):
        """Ensure the trigger function exists in the database."""
        await cursor.execute(
            sql.SQL("""
                CREATE OR REPLACE FUNCTION {function_name}()
                RETURNS TRIGGER AS $func$
                DECLARE
                    trigger_config RECORD;
                    job_data JSONB;
                BEGIN
                    -- Fetch trigger configuration
                    SELECT job_template INTO trigger_config
                    FROM {triggers_table}
                    WHERE schema_name = TG_TABLE_SCHEMA 
                      AND table_name = TG_TABLE_NAME 
                      AND trigger_name = REPLACE(TG_NAME, '_' || lower(TG_OP), '')
                      AND enabled = true
                      AND operations::jsonb ? TG_OP;
                    
                    IF NOT FOUND THEN
                        RETURN NULL;
                    END IF;
                    
                    job_data := trigger_config.job_template::jsonb;
                    job_data := jsonb_set(
                        job_data,
                        '{{m,trigger}}',
                        jsonb_build_object(
                            'operation', TG_OP,
                            'table_name', TG_TABLE_NAME,
                            'schema_name', TG_TABLE_SCHEMA,
                            'trigger_name', TG_NAME,
                            'timestamp', NOW()
                        )
                    );
                    
                    -- Insert job into queue
                    INSERT INTO {jobs_table} (
                        id,
                        queue,
                        func,
                        kwargs,
                        priority,
                        max_attempts, 
                        scheduled_at,
                        limits,
                        unique_key,
                        meta,
                        state,
                        created_at
                    ) VALUES (
                        gen_random_uuid(),
                        job_data->>'q',
                        job_data->>'f', 
                        COALESCE(job_data->'k', '{{}}'::jsonb),
                        COALESCE((job_data->>'p')::integer, 0),
                        COALESCE((job_data->>'a')::integer, 1),
                        COALESCE(
                            to_timestamp((job_data->>'s')::double precision),
                            NOW()
                        ),
                        COALESCE(job_data->'l', '[]'::jsonb),
                        job_data->>'u',
                        COALESCE(job_data->'m', '{{}}'::jsonb),
                        'pending',
                        NOW()
                    )
                    ON CONFLICT (unique_key)
                    WHERE
                        unique_key IS NOT NULL
                            AND state NOT IN ('succeeded', 'failed')
                    DO NOTHING;
                    PERFORM pg_notify(
                       {notify_channel},
                       json_build_object(
                           't', 'queue.pushed',
                           'q', job_data->>'q'
                       )::text
                    );
                    RETURN NULL;
                END;
                $func$ LANGUAGE plpgsql;
            """).format(
                function_name=sql.Identifier(f"{prefix}trigger_handler"),
                triggers_table=sql.Identifier(f"{prefix}triggers"),
                jobs_table=sql.Identifier(f"{prefix}jobs"),
                notify_channel=sql.Literal(f"{prefix}events"),
            )
        )

    @staticmethod
    async def _create_database_triggers(
        cursor,
        schema_name: str,
        table_name: str,
        trigger_name: str,
        operations: list[str],
        prefix: str,
    ):
        """Create PostgreSQL triggers for the specified operations."""
        for operation in operations:
            op_trigger_name = f"{trigger_name}_{operation.lower()}"

            await cursor.execute(
                sql.SQL("""
                    CREATE TRIGGER {trigger_name}
                    AFTER {operation} ON {schema}.{table}
                    FOR EACH STATEMENT EXECUTE FUNCTION {function_name}()
                """).format(
                    trigger_name=sql.Identifier(op_trigger_name),
                    operation=sql.SQL(operation.upper()),
                    schema=sql.Identifier(schema_name),
                    table=sql.Identifier(table_name),
                    function_name=sql.Identifier(f"{prefix}trigger_handler"),
                )
            )

    @staticmethod
    async def _drop_database_trigger(
        cursor, schema_name: str, table_name: str, trigger_name: str
    ):
        """Drop a PostgreSQL trigger if it exists."""
        await cursor.execute(
            sql.SQL("""
                DROP TRIGGER IF EXISTS {trigger_name} ON {schema}.{table}
            """).format(
                trigger_name=sql.Identifier(trigger_name),
                schema=sql.Identifier(schema_name),
                table=sql.Identifier(table_name),
            )
        )
