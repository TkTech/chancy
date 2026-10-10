import asyncio

import pytest
from psycopg import sql
from psycopg.rows import dict_row

from chancy import Chancy, Queue, QueuedJob, Worker, job
from chancy.plugins.leadership import ImmediateLeadership
from chancy.plugins.pruner import Pruner


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

    async with (
        chancy.pool.connection() as conn,
        conn.cursor(row_factory=dict_row) as cursor,
    ):
        await p.prune(chancy, cursor)

    pruned_job = await chancy.get_job(ref)
    assert pruned_job is None, "Job should be pruned"

    p = Pruner(
        (Pruner.Rules.Queue() == "test_queue") & (Pruner.Rules.Age() > 10)
    )
    ref = await chancy.push(job_to_run.job.with_queue("test_queue"))
    initial_job = await chancy.wait_for_job(ref)
    assert initial_job is not None, "Job should exist before pruning"

    async with (
        chancy.pool.connection() as conn,
        conn.cursor(row_factory=dict_row) as cursor,
    ):
        await p.prune(chancy, cursor)

    not_pruned_job = await chancy.get_job(ref)
    assert not_pruned_job is not None, "Job should not be pruned yet"

    await asyncio.sleep(10)

    async with (
        chancy.pool.connection() as conn,
        conn.cursor(row_factory=dict_row) as cursor,
    ):
        await p.prune(chancy, cursor)

    pruned_job = await chancy.get_job(ref)
    assert pruned_job is None, "Job should be pruned"


@pytest.mark.asyncio
async def test_pruner_only_prunes_finished_jobs(chancy: Chancy):
    """
    Only succeeded and failed jobs are pruned, even when unfinished jobs,
    such as those waiting to be retried, match the rule.
    """
    p = Pruner(Pruner.Rules.Queue() == "test_queue")
    await chancy.declare(Queue("test_queue"))

    refs = {
        state: await chancy.push(job_to_run.job.with_queue("test_queue"))
        for state in QueuedJob.State
    }

    async with (
        chancy.pool.connection() as conn,
        conn.cursor(row_factory=dict_row) as cursor,
    ):
        for state, ref in refs.items():
            await cursor.execute(
                sql.SQL("UPDATE {jobs} SET state = %s WHERE id = %s").format(
                    jobs=sql.Identifier(f"{chancy.prefix}jobs")
                ),
                [state.value, ref.identifier],
            )

        await p.prune(chancy, cursor)

    for state, ref in refs.items():
        should_prune = state in (
            QueuedJob.State.SUCCEEDED,
            QueuedJob.State.FAILED,
        )
        assert (await chancy.get_job(ref) is None) == should_prune, state
