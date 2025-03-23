import asyncio

import pytest
from psycopg.rows import dict_row

from chancy import Chancy, Queue, Worker, job
from chancy.plugins.pruner import Pruner
from chancy.plugins.leadership import ImmediateLeadership


@job()
def job_to_run():
    pass


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
async def test_pruner_functionality(chancy: Chancy, worker: Worker):
    """
    This test manually calls the prune method to avoid timing issues.
    """
    p = Pruner(Pruner.Rules.Queue() == "test_queue")
    await chancy.declare(Queue("test_queue"))

    ref = await chancy.push(job_to_run.job.with_queue("test_queue"))
    initial_job = await chancy.wait_for_job(ref)
    assert initial_job is not None, "Job should exist before pruning"

    async with chancy.pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cursor:
            await p.prune(chancy, cursor)

    pruned_job = await chancy.get_job(ref)
    assert pruned_job is None, "Job should be pruned"

    p = Pruner(
        (Pruner.Rules.Queue() == "test_queue") & (Pruner.Rules.Age() > 10)
    )
    ref = await chancy.push(job_to_run.job.with_queue("test_queue"))
    initial_job = await chancy.wait_for_job(ref)
    assert initial_job is not None, "Job should exist before pruning"

    async with chancy.pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cursor:
            await p.prune(chancy, cursor)

    not_pruned_job = await chancy.get_job(ref)
    assert not_pruned_job is not None, "Job should not be pruned yet"

    await asyncio.sleep(10)

    async with chancy.pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cursor:
            await p.prune(chancy, cursor)

    pruned_job = await chancy.get_job(ref)
    assert pruned_job is None, "Job should be pruned"
