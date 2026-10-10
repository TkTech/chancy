import asyncio

import pytest
from psycopg import AsyncConnection, sql

from chancy import Chancy, Queue, Worker, job


@job()
async def unique_job():
    pass


async def _push_all(chancy: Chancy, jobs):
    return [ref async for refs in chancy.push_many(jobs) for ref in refs]


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["push", "flush"])
async def test_regression_89(
    chancy: Chancy, worker_no_start: Worker, operation: str
):
    """
    Pushing jobs whose unique keys already exist takes row locks (the
    ON CONFLICT ... DO UPDATE clause), and so does the worker's batched job
    update. When the two transactions overlap and lock the rows in a
    different order, Postgres detects a deadlock and kills one of them.

    Hold the first row while a push or flush submits the rows in reverse
    order. Once that operation is blocked, locking the second row must not
    deadlock.
    """
    await chancy.declare(Queue("default"))
    jobs = [
        unique_job.job.with_unique_key("regression_89_b"),
        unique_job.job.with_unique_key("regression_89_a"),
    ]
    references = await _push_all(chancy, jobs)
    existing = await chancy.get_jobs(references)
    first, second = sorted(existing, key=lambda j: j.unique_key)
    if operation == "flush":
        for update in (second, first):
            await worker_no_start.queue_update(
                update.with_meta({"saved": True})
            )

    lock_row = sql.SQL("UPDATE {jobs} SET meta = meta WHERE id = %s").format(
        jobs=sql.Identifier(f"{chancy.prefix}jobs")
    )

    task = None
    try:
        async with (
            asyncio.timeout(5),
            await AsyncConnection.connect(chancy.dsn) as conn,
            conn.transaction(),
        ):
            await conn.execute("SET LOCAL deadlock_timeout = '100ms'")
            await conn.execute(lock_row, [first.id])

            task = asyncio.create_task(
                _push_all(chancy, jobs)
                if operation == "push"
                else worker_no_start.flush()
            )
            while True:
                cursor = await conn.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM pg_locks
                        WHERE NOT granted
                          AND %s = ANY(pg_blocking_pids(pid))
                    )
                    """,
                    [conn.info.backend_pid],
                )
                if (await cursor.fetchone())[0]:
                    break
                if task.done():
                    task.result()
                    pytest.fail("The operation should block on the first row")
                await asyncio.sleep(0.01)

            # Inconsistent ordering raises DeadlockDetected here or in task.
            await conn.execute(lock_row, [second.id])

        result = await asyncio.wait_for(task, timeout=5)
        if operation == "push":
            assert result == references
        else:
            assert all(
                job.meta == {"saved": True}
                for job in await chancy.get_jobs(references)
            )
    finally:
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("sync", [False, True])
async def test_regression_89_push_preserves_order(chancy: Chancy, sync: bool):
    """
    Re-ordering the inserts must not re-order the returned references.
    """
    await chancy.declare(Queue("default"))
    jobs = [
        unique_job.job.with_unique_key("regression_89_sync_c"),
        unique_job.job.with_unique_key("regression_89_sync_a"),
        unique_job.job.with_unique_key("regression_89_sync_b"),
        unique_job.job,
    ]
    if sync:
        with Chancy(chancy.dsn, prefix=chancy.prefix) as sync_chancy:
            references = [
                ref for refs in sync_chancy.sync_push_many(jobs) for ref in refs
            ]
    else:
        references = await _push_all(chancy, jobs)

    pushed = await chancy.get_jobs(references)
    by_id = {j.id: j for j in pushed}
    assert [by_id[ref.identifier].unique_key for ref in references] == [
        "regression_89_sync_c",
        "regression_89_sync_a",
        "regression_89_sync_b",
        None,
    ]
