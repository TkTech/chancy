import uuid
import pytest

import pytest_asyncio
from psycopg import sql

from chancy import job, Queue, Reference, Chancy, QueuedJob
from chancy.plugins.trigger import Trigger


@job(queue="trigger_events")
def handle_insert(*, j: QueuedJob):
    """Test job for INSERT operations"""
    assert j.meta["trigger"]["operation"] == "INSERT"
    assert j.meta["trigger"]["table_name"].startswith("test_users")
    assert j.meta["trigger"]["schema_name"] == "public"


@job(queue="trigger_events")
def handle_update(*, j: QueuedJob):
    """Test job for UPDATE operations"""
    assert j.meta["trigger"]["operation"] == "UPDATE"


@job(queue="trigger_events")
def handle_delete(*, j: QueuedJob):
    """Test job for DELETE operations"""
    assert j.meta["trigger"]["operation"] == "DELETE"


@job(queue="trigger_events")
def handle_any_change(*, j: QueuedJob):
    """Test job for any operation"""
    assert j.meta["trigger"]["operation"] in ["INSERT", "UPDATE", "DELETE"]


@job(queue="trigger_events", priority=10)
def handle_with_priority(*, j: QueuedJob):
    """Test job with priority"""
    assert j.meta["trigger"]["table_name"].startswith("test_users")


# Keep old name for backward compatibility with existing test
handle_table_change = handle_insert


@pytest_asyncio.fixture
async def test_table(chancy, test_suffix):
    # Use unique table name per xdist worker to avoid conflicts
    table_name = f"test_users{test_suffix}"

    async with chancy.pool.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {table} (
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL,
                        email TEXT UNIQUE NOT NULL
                    )
                """).format(table=sql.Identifier(table_name))
            )

    yield table_name

    async with chancy.pool.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                sql.SQL("DROP TABLE IF EXISTS {table} CASCADE").format(
                    table=sql.Identifier(table_name)
                )
            )


@pytest.mark.parametrize(
    "chancy",
    [{"plugins": [Trigger()], "no_default_plugins": True}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_trigger_creates_job_on_change(
    chancy: Chancy, worker, test_table
):
    """Test that database changes create jobs"""
    await chancy.declare(Queue("trigger_events"))

    # Register a trigger that listens for INSERT operations on the test table
    # and runs the handle_table_change job when such an operation occurs.
    trigger_id = await Trigger.register_trigger(
        chancy,
        table_name=test_table,
        operations=["INSERT"],
        job_template=handle_table_change,
    )

    async with chancy.pool.connection() as conn:
        async with conn.cursor() as cursor:
            async with conn.transaction():
                await cursor.execute(
                    sql.SQL("""
                        INSERT INTO {table} (name, email)
                        VALUES ('John Doe', 'john@example.com')
                    """).format(table=sql.Identifier(test_table))
                )

            await cursor.execute(
                sql.SQL("""
                    SELECT id
                    FROM {jobs_table}
                    WHERE func = %s
                    AND state = 'pending'
                """).format(jobs_table=sql.Identifier(f"{chancy.prefix}jobs")),
                (handle_table_change.job.func,),
            )

            jobs = await cursor.fetchall()
            assert len(jobs) == 1, "Trigger should create exactly one job"

            ref = Reference(jobs[0][0])
            j = await chancy.wait_for_job(ref)
            assert j, "Job did not complete successfully"

    # Now let us disable the trigger and ensure no new jobs are created
    await Trigger.disable_trigger(chancy, trigger_id)
    async with chancy.pool.connection() as conn:
        async with conn.cursor() as cursor:
            async with conn.transaction():
                await cursor.execute(
                    sql.SQL("""
                        INSERT INTO {table} (name, email)
                        VALUES ('Jane Doe', 'jane@example.com')
                    """).format(table=sql.Identifier(test_table))
                )

            await cursor.execute(
                sql.SQL("""
                    SELECT id
                    FROM {jobs_table}
                    WHERE func = %s
                    AND state = 'pending'
                """).format(jobs_table=sql.Identifier(f"{chancy.prefix}jobs")),
                (handle_table_change.job.func,),
            )

            jobs = await cursor.fetchall()
            assert len(jobs) == 0, (
                "Trigger should not create jobs when disabled"
            )


@pytest.mark.parametrize(
    "chancy",
    [{"plugins": [Trigger()], "no_default_plugins": True}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_trigger_update_operation(chancy: Chancy, worker, test_table):
    """Test that UPDATE operations create jobs"""
    await chancy.declare(Queue("trigger_events"))

    await Trigger.register_trigger(
        chancy,
        table_name=test_table,
        operations=["UPDATE"],
        job_template=handle_update,
    )

    async with chancy.pool.connection() as conn:
        async with conn.cursor() as cursor:
            # First insert a row
            async with conn.transaction():
                await cursor.execute(
                    sql.SQL("""
                        INSERT INTO {table} (name, email)
                        VALUES ('John Doe', 'john@example.com')
                    """).format(table=sql.Identifier(test_table))
                )

            # Now update it
            async with conn.transaction():
                await cursor.execute(
                    sql.SQL("""
                        UPDATE {table}
                        SET name = 'Jane Doe'
                        WHERE email = 'john@example.com'
                    """).format(table=sql.Identifier(test_table))
                )

            await cursor.execute(
                sql.SQL("""
                    SELECT id
                    FROM {jobs_table}
                    WHERE func = %s
                    AND state = 'pending'
                """).format(jobs_table=sql.Identifier(f"{chancy.prefix}jobs")),
                (handle_update.job.func,),
            )

            jobs = await cursor.fetchall()
            assert len(jobs) == 1, (
                "UPDATE trigger should create exactly one job"
            )

            ref = Reference(jobs[0][0])
            j = await chancy.wait_for_job(ref)
            assert j, "Job did not complete successfully"


@pytest.mark.parametrize(
    "chancy",
    [{"plugins": [Trigger()], "no_default_plugins": True}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_trigger_delete_operation(chancy: Chancy, worker, test_table):
    """Test that DELETE operations create jobs"""
    await chancy.declare(Queue("trigger_events"))

    await Trigger.register_trigger(
        chancy,
        table_name=test_table,
        operations=["DELETE"],
        job_template=handle_delete,
    )

    async with chancy.pool.connection() as conn:
        async with conn.cursor() as cursor:
            # First insert a row
            async with conn.transaction():
                await cursor.execute(
                    sql.SQL("""
                        INSERT INTO {table} (name, email)
                        VALUES ('John Doe', 'john@example.com')
                    """).format(table=sql.Identifier(test_table))
                )

            # Now delete it
            async with conn.transaction():
                await cursor.execute(
                    sql.SQL("""
                        DELETE FROM {table}
                        WHERE email = 'john@example.com'
                    """).format(table=sql.Identifier(test_table))
                )

            await cursor.execute(
                sql.SQL("""
                    SELECT id
                    FROM {jobs_table}
                    WHERE func = %s
                    AND state = 'pending'
                """).format(jobs_table=sql.Identifier(f"{chancy.prefix}jobs")),
                (handle_delete.job.func,),
            )

            jobs = await cursor.fetchall()
            assert len(jobs) == 1, (
                "DELETE trigger should create exactly one job"
            )

            ref = Reference(jobs[0][0])
            j = await chancy.wait_for_job(ref)
            assert j, "Job did not complete successfully"


@pytest.mark.parametrize(
    "chancy",
    [{"plugins": [Trigger()], "no_default_plugins": True}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_trigger_multiple_operations(chancy: Chancy, worker, test_table):
    """Test trigger with multiple operations (INSERT, UPDATE, DELETE)"""
    await chancy.declare(Queue("trigger_events"))

    await Trigger.register_trigger(
        chancy,
        table_name=test_table,
        operations=["INSERT", "UPDATE", "DELETE"],
        job_template=handle_any_change,
    )

    async with chancy.pool.connection() as conn:
        async with conn.cursor() as cursor:
            # INSERT
            async with conn.transaction():
                await cursor.execute(
                    sql.SQL("""
                        INSERT INTO {table} (name, email)
                        VALUES ('John Doe', 'john@example.com')
                    """).format(table=sql.Identifier(test_table))
                )

            # UPDATE
            async with conn.transaction():
                await cursor.execute(
                    sql.SQL("""
                        UPDATE {table}
                        SET name = 'Jane Doe'
                        WHERE email = 'john@example.com'
                    """).format(table=sql.Identifier(test_table))
                )

            # DELETE
            async with conn.transaction():
                await cursor.execute(
                    sql.SQL("""
                        DELETE FROM {table}
                        WHERE email = 'john@example.com'
                    """).format(table=sql.Identifier(test_table))
                )

            # Query for all jobs created by this trigger (any state)
            await cursor.execute(
                sql.SQL("""
                    SELECT id
                    FROM {jobs_table}
                    WHERE func = %s
                    ORDER BY created_at ASC
                """).format(jobs_table=sql.Identifier(f"{chancy.prefix}jobs")),
                (handle_any_change.job.func,),
            )

            jobs = await cursor.fetchall()
            assert len(jobs) == 3, (
                f"Should create 3 jobs (INSERT, UPDATE, DELETE), got {len(jobs)}"
            )

            # Wait for all jobs to complete
            for job_row in jobs:
                ref = Reference(job_row[0])
                j = await chancy.wait_for_job(ref)
                assert j, "Job did not complete successfully"


@pytest.mark.parametrize(
    "chancy",
    [{"plugins": [Trigger()], "no_default_plugins": True}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_unregister_trigger(chancy: Chancy, worker, test_table):
    """Test that unregistering a trigger removes it completely"""
    await chancy.declare(Queue("trigger_events"))

    trigger_id = await Trigger.register_trigger(
        chancy,
        table_name=test_table,
        operations=["INSERT"],
        job_template=handle_insert,
    )

    # Verify trigger exists
    triggers = await Trigger.get_triggers(chancy, trigger_ids=[trigger_id])
    assert len(triggers) == 1
    assert trigger_id in triggers
    assert triggers[trigger_id].enabled is True

    # Unregister it
    await Trigger.unregister_trigger(chancy, trigger_id)

    # Verify it's gone
    triggers = await Trigger.get_triggers(chancy, trigger_ids=[trigger_id])
    assert len(triggers) == 0

    # Verify no jobs are created
    async with chancy.pool.connection() as conn:
        async with conn.cursor() as cursor:
            async with conn.transaction():
                await cursor.execute(
                    sql.SQL("""
                        INSERT INTO {table} (name, email)
                        VALUES ('John Doe', 'john@example.com')
                    """).format(table=sql.Identifier(test_table))
                )

            await cursor.execute(
                sql.SQL("""
                    SELECT id
                    FROM {jobs_table}
                    WHERE func = %s
                    AND state = 'pending'
                """).format(jobs_table=sql.Identifier(f"{chancy.prefix}jobs")),
                (handle_insert.job.func,),
            )

            jobs = await cursor.fetchall()
            assert len(jobs) == 0, "Unregistered trigger should not create jobs"


@pytest.mark.parametrize(
    "chancy",
    [{"plugins": [Trigger()], "no_default_plugins": True}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_enable_trigger(chancy: Chancy, worker, test_table):
    """Test re-enabling a disabled trigger"""
    await chancy.declare(Queue("trigger_events"))

    trigger_id = await Trigger.register_trigger(
        chancy,
        table_name=test_table,
        operations=["INSERT"],
        job_template=handle_insert,
    )

    # Disable it
    changed = await Trigger.disable_trigger(chancy, trigger_id)
    assert changed is True

    # Verify no jobs created when disabled
    async with chancy.pool.connection() as conn:
        async with conn.cursor() as cursor:
            async with conn.transaction():
                await cursor.execute(
                    sql.SQL("""
                        INSERT INTO {table} (name, email)
                        VALUES ('Disabled User', 'disabled@example.com')
                    """).format(table=sql.Identifier(test_table))
                )

            await cursor.execute(
                sql.SQL("""
                    SELECT id
                    FROM {jobs_table}
                    WHERE func = %s
                    AND state = 'pending'
                """).format(jobs_table=sql.Identifier(f"{chancy.prefix}jobs")),
                (handle_insert.job.func,),
            )

            jobs = await cursor.fetchall()
            assert len(jobs) == 0

    # Re-enable it
    changed = await Trigger.enable_trigger(chancy, trigger_id)
    assert changed is True

    # Verify jobs are created again
    async with chancy.pool.connection() as conn:
        async with conn.cursor() as cursor:
            async with conn.transaction():
                await cursor.execute(
                    sql.SQL("""
                        INSERT INTO {table} (name, email)
                        VALUES ('Enabled User', 'enabled@example.com')
                    """).format(table=sql.Identifier(test_table))
                )

            await cursor.execute(
                sql.SQL("""
                    SELECT id
                    FROM {jobs_table}
                    WHERE func = %s
                    AND state = 'pending'
                """).format(jobs_table=sql.Identifier(f"{chancy.prefix}jobs")),
                (handle_insert.job.func,),
            )

            jobs = await cursor.fetchall()
            assert len(jobs) == 1, "Re-enabled trigger should create jobs"


@pytest.mark.parametrize(
    "chancy",
    [{"plugins": [Trigger()], "no_default_plugins": True}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_get_triggers(chancy: Chancy, worker, test_table):
    """Test retrieving trigger information"""
    await chancy.declare(Queue("trigger_events"))

    # Create multiple triggers
    trigger_id1 = await Trigger.register_trigger(
        chancy,
        table_name=test_table,
        operations=["INSERT"],
        job_template=handle_insert,
    )

    trigger_id2 = await Trigger.register_trigger(
        chancy,
        table_name=test_table,
        operations=["UPDATE"],
        job_template=handle_update,
    )

    # Get all triggers
    all_triggers = await Trigger.get_triggers(chancy)
    assert len(all_triggers) >= 2

    # Get specific triggers
    specific_triggers = await Trigger.get_triggers(
        chancy, trigger_ids=[trigger_id1, trigger_id2]
    )
    assert len(specific_triggers) == 2
    assert trigger_id1 in specific_triggers
    assert trigger_id2 in specific_triggers

    # Verify trigger data
    trigger1 = specific_triggers[trigger_id1]
    assert trigger1.table_name == test_table
    assert trigger1.schema_name == "public"
    assert trigger1.operations == ["INSERT"]
    assert trigger1.enabled is True
    assert trigger1.job_template.func == handle_insert.job.func


@pytest.mark.parametrize(
    "chancy",
    [{"plugins": [Trigger()], "no_default_plugins": True}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_trigger_invalid_operation(chancy: Chancy, worker, test_table):
    """Test that invalid operations raise ValueError"""
    await chancy.declare(Queue("trigger_events"))

    with pytest.raises(ValueError, match="Invalid operation"):
        await Trigger.register_trigger(
            chancy,
            table_name=test_table,
            operations=["INVALID_OP"],
            job_template=handle_insert,
        )


@pytest.mark.parametrize(
    "chancy",
    [{"plugins": [Trigger()], "no_default_plugins": True}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_unregister_nonexistent_trigger(
    chancy: Chancy, worker, test_table
):
    """Test that unregistering a non-existent trigger raises ValueError"""
    nonexistent_id = str(uuid.uuid4())
    with pytest.raises(ValueError, match="Trigger .* not found"):
        await Trigger.unregister_trigger(chancy, nonexistent_id)


@pytest.mark.parametrize(
    "chancy",
    [{"plugins": [Trigger()], "no_default_plugins": True}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_enable_nonexistent_trigger(chancy: Chancy, worker, test_table):
    """Test that enabling a non-existent trigger raises ValueError"""
    nonexistent_id = str(uuid.uuid4())
    with pytest.raises(ValueError, match="Trigger .* not found"):
        await Trigger.enable_trigger(chancy, nonexistent_id)


@pytest.mark.parametrize(
    "chancy",
    [{"plugins": [Trigger()], "no_default_plugins": True}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_disable_nonexistent_trigger(chancy: Chancy, worker, test_table):
    """Test that disabling a non-existent trigger raises ValueError"""
    nonexistent_id = str(uuid.uuid4())
    with pytest.raises(ValueError, match="Trigger .* not found"):
        await Trigger.disable_trigger(chancy, nonexistent_id)


@pytest.mark.parametrize(
    "chancy",
    [{"plugins": [Trigger()], "no_default_plugins": True}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_trigger_bulk_operations(chancy: Chancy, worker, test_table):
    """Test that statement-level triggers fire once per statement, not per row"""
    await chancy.declare(Queue("trigger_events"))

    await Trigger.register_trigger(
        chancy,
        table_name=test_table,
        operations=["INSERT"],
        job_template=handle_insert,
    )

    async with chancy.pool.connection() as conn:
        async with conn.cursor() as cursor:
            # Bulk insert 5 rows in one statement
            async with conn.transaction():
                await cursor.execute(
                    sql.SQL("""
                        INSERT INTO {table} (name, email)
                        VALUES
                            ('User 1', 'user1@example.com'),
                            ('User 2', 'user2@example.com'),
                            ('User 3', 'user3@example.com'),
                            ('User 4', 'user4@example.com'),
                            ('User 5', 'user5@example.com')
                    """).format(table=sql.Identifier(test_table))
                )

            await cursor.execute(
                sql.SQL("""
                    SELECT id
                    FROM {jobs_table}
                    WHERE func = %s
                    AND state = 'pending'
                """).format(jobs_table=sql.Identifier(f"{chancy.prefix}jobs")),
                (handle_insert.job.func,),
            )

            jobs = await cursor.fetchall()
            # Statement-level trigger should fire once per statement, not per row
            assert len(jobs) == 1, (
                "Statement-level trigger should create 1 job for bulk insert"
            )


@pytest.mark.parametrize(
    "chancy",
    [{"plugins": [Trigger()], "no_default_plugins": True}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_trigger_with_job_parameters(chancy: Chancy, worker, test_table):
    """Test trigger with job template that has priority and other parameters"""
    await chancy.declare(Queue("trigger_events"))

    await Trigger.register_trigger(
        chancy,
        table_name=test_table,
        operations=["INSERT"],
        job_template=handle_with_priority,
    )

    async with chancy.pool.connection() as conn:
        async with conn.cursor() as cursor:
            async with conn.transaction():
                await cursor.execute(
                    sql.SQL("""
                        INSERT INTO {table} (name, email)
                        VALUES ('Priority User', 'priority@example.com')
                    """).format(table=sql.Identifier(test_table))
                )

            await cursor.execute(
                sql.SQL("""
                    SELECT id, priority
                    FROM {jobs_table}
                    WHERE func = %s
                    AND state = 'pending'
                """).format(jobs_table=sql.Identifier(f"{chancy.prefix}jobs")),
                (handle_with_priority.job.func,),
            )

            jobs = await cursor.fetchall()
            assert len(jobs) == 1
            assert jobs[0][1] == 10, "Job should have priority 10"


@pytest.mark.parametrize(
    "chancy",
    [{"plugins": [Trigger()], "no_default_plugins": True}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_trigger_disabled_at_registration(
    chancy: Chancy, worker, test_table
):
    """Test registering a trigger with enabled=False"""
    await chancy.declare(Queue("trigger_events"))

    trigger_id = await Trigger.register_trigger(
        chancy,
        table_name=test_table,
        operations=["INSERT"],
        job_template=handle_insert,
        enabled=False,
    )

    # Verify trigger is disabled
    triggers = await Trigger.get_triggers(chancy, trigger_ids=[trigger_id])
    assert trigger_id in triggers
    assert triggers[trigger_id].enabled is False

    # Verify no jobs are created
    async with chancy.pool.connection() as conn:
        async with conn.cursor() as cursor:
            async with conn.transaction():
                await cursor.execute(
                    sql.SQL("""
                        INSERT INTO {table} (name, email)
                        VALUES ('Test User', 'test@example.com')
                    """).format(table=sql.Identifier(test_table))
                )

            await cursor.execute(
                sql.SQL("""
                    SELECT id
                    FROM {jobs_table}
                    WHERE func = %s
                    AND state = 'pending'
                """).format(jobs_table=sql.Identifier(f"{chancy.prefix}jobs")),
                (handle_insert.job.func,),
            )

            jobs = await cursor.fetchall()
            assert len(jobs) == 0, "Disabled trigger should not create jobs"


@pytest_asyncio.fixture
async def test_schema(chancy, test_suffix):
    """Create a test schema for schema-specific tests"""
    # Use unique schema/table names per xdist worker
    schema_name = f"test_schema{test_suffix}"
    table_name = f"test_users{test_suffix}"

    async with chancy.pool.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema}").format(
                    schema=sql.Identifier(schema_name)
                )
            )
            await cursor.execute(
                sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {schema}.{table} (
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL,
                        email TEXT UNIQUE NOT NULL
                    )
                """).format(
                    schema=sql.Identifier(schema_name),
                    table=sql.Identifier(table_name),
                )
            )

    yield (schema_name, table_name)

    async with chancy.pool.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {schema} CASCADE").format(
                    schema=sql.Identifier(schema_name)
                )
            )


@pytest.mark.parametrize(
    "chancy",
    [{"plugins": [Trigger()], "no_default_plugins": True}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_trigger_different_schema(chancy: Chancy, worker, test_schema):
    """Test trigger on a table in a non-public schema"""
    schema_name, table_name = test_schema
    await chancy.declare(Queue("trigger_events"))

    await Trigger.register_trigger(
        chancy,
        table_name=table_name,
        operations=["INSERT"],
        job_template=handle_insert,
        schema_name=schema_name,
    )

    async with chancy.pool.connection() as conn:
        async with conn.cursor() as cursor:
            async with conn.transaction():
                await cursor.execute(
                    sql.SQL("""
                        INSERT INTO {schema}.{table} (name, email)
                        VALUES ('Schema User', 'schema@example.com')
                    """).format(
                        schema=sql.Identifier(schema_name),
                        table=sql.Identifier(table_name),
                    )
                )

            await cursor.execute(
                sql.SQL("""
                    SELECT id
                    FROM {jobs_table}
                    WHERE func = %s
                    AND state = 'pending'
                """).format(jobs_table=sql.Identifier(f"{chancy.prefix}jobs")),
                (handle_insert.job.func,),
            )

            jobs = await cursor.fetchall()
            assert len(jobs) == 1, "Trigger in custom schema should create job"
