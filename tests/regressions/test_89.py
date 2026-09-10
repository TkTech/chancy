import asyncio

import pytest
from psycopg import AsyncConnection, sql

from chancy import Chancy, job
from chancy.utils import lock_order_key


@job()
async def unique_job():
    pass


async def _push_all(chancy: Chancy, jobs):
    return [ref async for refs in chancy.push_many(jobs) for ref in refs]


@pytest.mark.asyncio
async def test_regression_89(chancy: Chancy):
    """
    Pushing jobs whose unique keys already exist takes row locks (the
    ON CONFLICT ... DO UPDATE clause), and so does the worker's batched job
    update. When the two transactions overlap and lock the rows in a
    different order, Postgres detects a deadlock and kills one of them.

    This test replays the interleaving deterministically: one transaction
    locks the rows in the order the worker uses, while a concurrent push
    submits the same unique keys in the opposite order.
    """
    jobs = [
        unique_job.job.with_unique_key("regression_89_b"),
        unique_job.job.with_unique_key("regression_89_a"),
    ]
    references = await _push_all(chancy, jobs)
    existing = await chancy.get_jobs(references)
    # Same ordering rule as Worker._maintain_updates.
    first, second = sorted(
        existing, key=lambda j: (lock_order_key(j.unique_key), j.id)
    )

    lock_row = sql.SQL("UPDATE {jobs} SET meta = meta WHERE id = %s").format(
        jobs=sql.Identifier(f"{chancy.prefix}jobs")
    )

    async with (
        await AsyncConnection.connect(chancy.dsn) as conn,
        conn.transaction(),
    ):
        await conn.execute("SET LOCAL deadlock_timeout = '100ms'")
        await conn.execute(lock_row, [first.id])

        push = asyncio.create_task(_push_all(chancy, jobs))
        await asyncio.sleep(0.5)
        assert not push.done(), "the push should be waiting on the first row"

        # With inconsistent lock ordering, this raises DeadlockDetected here
        # or in the push task.
        await conn.execute(lock_row, [second.id])

    assert await push == references


@pytest.mark.asyncio
async def test_regression_89_sync_push_preserves_order(chancy: Chancy):
    """
    Re-ordering the inserts must not re-order the returned references.
    """
    jobs = [
        unique_job.job.with_unique_key("regression_89_sync_c"),
        unique_job.job.with_unique_key("regression_89_sync_a"),
        unique_job.job.with_unique_key("regression_89_sync_b"),
        unique_job.job,
    ]
    with Chancy(chancy.dsn, prefix=chancy.prefix) as sync_chancy:
        references = [
            ref for refs in sync_chancy.sync_push_many(jobs) for ref in refs
        ]

    pushed = await chancy.get_jobs(references)
    by_id = {j.id: j for j in pushed}
    assert [by_id[ref.identifier].unique_key for ref in references] == [
        "regression_89_sync_c",
        "regression_89_sync_a",
        "regression_89_sync_b",
        None,
    ]
