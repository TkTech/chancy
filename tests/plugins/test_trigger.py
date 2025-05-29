import pytest

import pytest_asyncio

from chancy import job, Queue, Reference, Chancy, QueuedJob
from chancy.plugins.trigger import Trigger


@job(queue="trigger_events")
def handle_table_change(*, j: QueuedJob):
    """Test job for table changes"""
    assert j.meta["trigger"]["operation"] == "INSERT"
    assert j.meta["trigger"]["table_name"] == "test_users"
    assert j.meta["trigger"]["schema_name"] == "public"


@pytest_asyncio.fixture
async def test_table(chancy):
    table_name = "test_users"

    async with chancy.pool.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE NOT NULL
                )
            """)

    yield table_name

    async with chancy.pool.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(f"DROP TABLE IF EXISTS {table_name}")


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
        job_template=handle_table_change,
        operations=["INSERT"],
    )

    async with chancy.pool.connection() as conn:
        async with conn.cursor() as cursor:
            async with conn.transaction():
                await cursor.execute(f"""
                    INSERT INTO {test_table} (name, email)
                    VALUES ('John Doe', 'john@example.com')
                """)

            await cursor.execute(
                f"""
                SELECT id
                FROM {chancy.prefix}jobs
                WHERE func = %s
                AND state = 'pending'
            """,
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
                await cursor.execute(f"""
                    INSERT INTO {test_table} (name, email)
                    VALUES ('Jane Doe', 'jane@example.com')
                """)

            await cursor.execute(
                f"""
                SELECT 
                FROM {chancy.prefix}jobs
                WHERE func = %s
                AND state = 'pending'
            """,
                (handle_table_change.job.func,),
            )

            jobs = await cursor.fetchall()
            assert (
                len(jobs) == 0
            ), "Trigger should not create jobs when disabled"
