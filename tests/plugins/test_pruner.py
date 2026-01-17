import asyncio

import pytest
from psycopg.rows import dict_row

from chancy import Chancy, Queue, Worker, job
from chancy.job import ConcurrencyRule
from chancy.plugins.leadership import ImmediateLeadership
from chancy.plugins.pruner import Pruner


@job()
def job_to_run():
    pass


@job(concurrency_rule=ConcurrencyRule(2, "user_id"))
def job_with_concurrency(user_id: int):
    pass


async def _add_old_concurrency_rule(
    chancy: Chancy, key: str, max_concurrency: int
):
    """Helper to add an old concurrency rule directly to the database."""
    async with chancy.pool.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                f"""
                INSERT INTO {chancy.prefix}concurrency_configs 
                (concurrency_key, concurrency_max, created_at, updated_at)
                VALUES (%s, %s, NOW() - INTERVAL '8 days', NOW() - INTERVAL '8 days')
                """,
                (key, max_concurrency),
            )
        await conn.commit()


async def _count_concurrency_rules(chancy: Chancy) -> int:
    """Helper to count concurrency rules in the database."""
    async with chancy.pool.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                f"SELECT COUNT(*) FROM {chancy.prefix}concurrency_configs"
            )
            result = await cursor.fetchone()
            return result[0] if result else 0


@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [
                ImmediateLeadership(),
            ],
            "no_default_plugins": True,
        }
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_job_rule_pruning(chancy: Chancy, worker: Worker):
    """
    This test manually calls the prune method to avoid timing issues.
    """
    p = Pruner(
        job_rule=Pruner.JobRules.Queue() == "test_queue", concurrency_rule=None
    )
    await chancy.declare(Queue("test_queue"))

    ref = await chancy.push(job_to_run.job.with_queue("test_queue"))
    initial_job = await chancy.wait_for_job(ref)
    assert initial_job is not None, "Job should exist before pruning"

    async with chancy.pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cursor:
            await p.prune_jobs(chancy, cursor)

    pruned_job = await chancy.get_job(ref)
    assert pruned_job is None, "Job should be pruned"

    p = Pruner(
        job_rule=(Pruner.JobRules.Queue() == "test_queue")
        & (Pruner.JobRules.Age() > 10),
        concurrency_rule=None,
    )
    ref = await chancy.push(job_to_run.job.with_queue("test_queue"))
    initial_job = await chancy.wait_for_job(ref)
    assert initial_job is not None, "Job should exist before pruning"

    async with chancy.pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cursor:
            await p.prune_jobs(chancy, cursor)

    not_pruned_job = await chancy.get_job(ref)
    assert not_pruned_job is not None, "Job should not be pruned yet"

    await asyncio.sleep(10)

    async with chancy.pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cursor:
            await p.prune_jobs(chancy, cursor)

    pruned_job = await chancy.get_job(ref)
    assert pruned_job is None, "Job should be pruned"


@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [
                ImmediateLeadership(),
            ],
            "no_default_plugins": True,
        }
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_concurrency_rule_pruning_by_age(chancy: Chancy, worker: Worker):
    """Test pruning concurrency rules older than a certain age."""
    # Create a pruner that cleans rules older than 3 days
    p = Pruner(
        job_rule=None,
        concurrency_rule=Pruner.ConcurrencyRules.Age()
        > 60 * 60 * 24 * 3,  # 3 days
    )

    # Add an old concurrency rule (8 days old)
    await _add_old_concurrency_rule(
        chancy, "test.job_with_concurrency:user_123", 2
    )

    # Verify concurrency rule exists
    initial_count = await _count_concurrency_rules(chancy)
    assert initial_count == 1, "Concurrency rule should exist before pruning"

    # Run concurrency rule pruning
    async with chancy.pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cursor:
            rows_removed = await p.prune_concurrency_rules(chancy, cursor)

    # Verify concurrency rule was pruned
    assert rows_removed == 1, "Should have removed 1 concurrency rule"
    final_count = await _count_concurrency_rules(chancy)
    assert final_count == 0, "Concurrency rule should be pruned"


@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [
                ImmediateLeadership(),
            ],
            "no_default_plugins": True,
        }
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_concurrency_rule_pruning_orphaned(
    chancy: Chancy, worker: Worker
):
    """Test pruning orphaned concurrency rules (no corresponding jobs)."""
    # Create a pruner that only cleans orphaned concurrency rules
    p = Pruner(
        job_rule=None,
        concurrency_rule=Pruner.ConcurrencyRules.Orphaned(),
    )

    # Add a concurrency rule without any corresponding jobs
    await _add_old_concurrency_rule(chancy, "test.orphaned_job:user_456", 3)

    # Also add a config that will have a corresponding job
    await chancy.declare(Queue("test_queue"))
    await chancy.push(
        job_with_concurrency.job.with_queue("test_queue").with_kwargs(
            user_id=789
        )
    )

    # Verify both concurrency rules exist
    initial_count = await _count_concurrency_rules(chancy)
    assert initial_count == 2, "Should have 2 concurrency rules before pruning"

    # Run concurrency rule pruning
    async with chancy.pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cursor:
            rows_removed = await p.prune_concurrency_rules(chancy, cursor)

    # Verify only orphaned concurrency rule was pruned
    assert rows_removed == 1, "Should have removed 1 orphaned concurrency rule"
    final_count = await _count_concurrency_rules(chancy)
    assert final_count == 1, (
        "Should have 1 concurrency rule remaining (non-orphaned)"
    )


@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [
                ImmediateLeadership(),
            ],
            "no_default_plugins": True,
        }
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_concurrency_rule_pruning_combined_rules(
    chancy: Chancy, worker: Worker
):
    """Test pruning with combined rules (age OR orphaned)."""
    # Create a pruner with combined rules: old OR orphaned
    p = Pruner(
        job_rule=None,
        concurrency_rule=(
            (Pruner.ConcurrencyRules.Age() > 60 * 60 * 24 * 3)  # 3 days
            | Pruner.ConcurrencyRules.Orphaned()
        ),
    )

    # Add an old config (8 days)
    await _add_old_concurrency_rule(chancy, "test.old_job:user_111", 1)

    # Add an orphaned config (recent but no jobs)
    async with chancy.pool.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                f"""
                INSERT INTO {chancy.prefix}concurrency_configs 
                (concurrency_key, concurrency_max, created_at, updated_at)
                VALUES (%s, %s, NOW(), NOW())
                """,
                ("test.orphaned_recent:user_222", 2),
            )
        await conn.commit()

    # Add a fresh concurrency rule with corresponding job
    await chancy.declare(Queue("test_queue"))
    await chancy.push(
        job_with_concurrency.job.with_queue("test_queue").with_kwargs(
            user_id=333
        )
    )

    # Verify all concurrency rules exist
    initial_count = await _count_concurrency_rules(chancy)
    assert initial_count == 3, "Should have 3 concurrency rules before pruning"

    # Run concurrency rule pruning
    async with chancy.pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cursor:
            rows_removed = await p.prune_concurrency_rules(chancy, cursor)

    # Verify old and orphaned concurrency rules were pruned, but fresh one with job remains
    assert rows_removed == 2, (
        "Should have removed 2 concurrency rules (old + orphaned)"
    )
    final_count = await _count_concurrency_rules(chancy)
    assert final_count == 1, (
        "Should have 1 concurrency rule remaining (fresh with job)"
    )
