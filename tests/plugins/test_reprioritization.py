import asyncio
import datetime

import pytest
from psycopg import sql

from chancy import Job, Queue, Chancy, Worker
from chancy.plugins.leadership import ImmediateLeadership
from chancy.plugins.reprioritize import Reprioritize
from chancy.rule import JobRules


def simple_job():
    pass


@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [
                ImmediateLeadership(),
                Reprioritize(
                    JobRules.Age() > 0,
                    check_interval=1,
                    priority_increase=5,
                ),
            ],
            "no_default_plugins": True,
        }
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_basic_reprioritization(chancy: Chancy, worker: Worker):
    """
    Tests that basic reprioritization works as expected.
    """
    await chancy.declare(Queue("default"))

    ref = await chancy.push(
        Job.from_func(simple_job).with_scheduled_at(
            datetime.datetime.now(tz=datetime.timezone.utc)
            + datetime.timedelta(minutes=10)
        )
    )

    initial_job = await chancy.get_job(ref)
    initial_priority = initial_job.priority

    await asyncio.sleep(2)

    updated_job = await chancy.get_job(ref)

    # Priority should have increased (making it more important)
    assert updated_job.priority > initial_priority


@pytest.mark.asyncio
async def test_reprioritization_updates_each_job_once(chancy: Chancy):
    plugin = Reprioritize(
        JobRules.Queue() == "default",
        priority_increase=5,
        batch_size=2,
    )
    references = []
    async for batch in chancy.push_many(
        [Job.from_func(simple_job) for _ in range(5)]
    ):
        references.extend(batch)

    updated = await asyncio.wait_for(
        plugin.reprioritize_jobs(chancy), timeout=2
    )

    assert updated == 5
    jobs = await chancy.get_jobs(references)
    assert all(job.priority == 5 for job in jobs)

    updated = await asyncio.wait_for(
        plugin.reprioritize_jobs(chancy), timeout=2
    )

    assert updated == 5
    jobs = await chancy.get_jobs(references)
    assert all(job.priority == 10 for job in jobs)


@pytest.mark.asyncio
async def test_reprioritization_with_percent_in_rule(chancy: Chancy):
    plugin = Reprioritize(
        JobRules.Job().contains("simple_job"),
        priority_increase=5,
        batch_size=1,
    )
    references = [
        await chancy.push(Job.from_func(simple_job)),
        await chancy.push(Job.from_func(simple_job)),
    ]

    updated = await plugin.reprioritize_jobs(chancy)

    assert updated == 2
    jobs = await chancy.get_jobs(references)
    assert all(job.priority == 5 for job in jobs)


@pytest.mark.asyncio
async def test_reprioritization_skips_locked_jobs(chancy: Chancy):
    plugin = Reprioritize(
        JobRules.Queue() == "default",
        priority_increase=5,
        batch_size=2,
    )
    references = []
    async for batch in chancy.push_many(
        [Job.from_func(simple_job) for _ in range(3)]
    ):
        references.extend(batch)

    async with chancy.pool.connection() as conn:
        async with conn.transaction():
            await conn.execute(
                sql.SQL(
                    """
                    SELECT id
                    FROM {jobs_table}
                    WHERE id = %(id)s
                    FOR UPDATE
                    """
                ).format(jobs_table=sql.Identifier(f"{chancy.prefix}jobs")),
                {"id": references[0].identifier},
            )

            updated = await asyncio.wait_for(
                plugin.reprioritize_jobs(chancy), timeout=2
            )

            assert updated == 2

    updated = await asyncio.wait_for(
        plugin.reprioritize_jobs(chancy), timeout=2
    )

    assert updated == 3
    jobs = {job.id: job for job in await chancy.get_jobs(references)}
    assert jobs[references[0].identifier].priority == 5
    assert all(
        jobs[reference.identifier].priority == 10
        for reference in references[1:]
    )


@pytest.mark.parametrize("batch_size", [0, -1])
def test_reprioritization_requires_positive_batch_size(batch_size: int):
    with pytest.raises(ValueError, match="batch_size must be greater than 0"):
        Reprioritize(JobRules.Age() > 0, batch_size=batch_size)
