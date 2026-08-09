import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from psycopg import sql

from chancy.app import Chancy
from chancy.plugins.leadership import ImmediateLeadership, Leadership
from chancy.worker import Worker


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
async def test_immediate_leadership(chancy: Chancy, worker: Worker):
    """
    Ensures that the immediate leadership plugin works as expected.
    """
    assert worker.is_leader.is_set()


@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [
                Leadership(poll_interval=5),
            ],
            "no_default_plugins": True,
        }
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_leadership(chancy: Chancy, worker: Worker):
    """
    Ensures that the leadership plugin works as expected.
    """
    await asyncio.sleep(10)
    assert worker.is_leader.is_set()


@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [
                Leadership(poll_interval=60, timeout=120),
            ],
            "no_default_plugins": True,
        }
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_leadership_renewal_requires_ownership(
    chancy: Chancy, worker: Worker
):
    """
    Ensures that a stale leader cannot renew another worker's lease.
    """
    leadership = chancy.plugins[Leadership.get_identifier()]

    gained = asyncio.create_task(
        worker.hub.wait_for("leadership.gained", timeout=5)
    )
    await asyncio.sleep(0)
    leadership.wake_up()
    assert await gained
    assert worker.is_leader.is_set()

    other_worker_id = "another-worker"
    other_expiry = datetime.now(tz=UTC) + timedelta(hours=1)
    leader_table = sql.Identifier(f"{chancy.prefix}leader")
    async with chancy.pool.connection() as conn, conn.cursor() as cursor:
        await cursor.execute(
            sql.SQL(
                """
                    UPDATE {leader}
                    SET worker_id = %s, expires_at = %s
                    WHERE id = 1
                    """
            ).format(leader=leader_table),
            (other_worker_id, other_expiry),
        )

    result = asyncio.create_task(
        worker.hub.wait_for(
            ["leadership.lost", "leadership.renewed"], timeout=5
        )
    )
    await asyncio.sleep(0)
    leadership.wake_up()
    events = await result
    assert events

    async with chancy.pool.connection() as conn, conn.cursor() as cursor:
        await cursor.execute(
            sql.SQL(
                """
                    SELECT worker_id, expires_at
                    FROM {leader}
                    WHERE id = 1
                    """
            ).format(leader=leader_table)
        )
        recorded_worker_id, recorded_expiry = await cursor.fetchone()

    assert recorded_worker_id == other_worker_id
    assert recorded_expiry == other_expiry
    assert events[0].name == "leadership.lost"
    assert not worker.is_leader.is_set()


@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [
                Leadership(poll_interval=5, timeout=10),
            ],
            "no_default_plugins": True,
        }
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_leadership_many_workers(chancy: Chancy):
    """
    Ensures that the leadership plugin works as expected with several workers.
    """
    workers = [Worker(chancy) for _ in range(10)]
    for worker in workers:
        await worker.start()

    await asyncio.sleep(15)
    leaders = [worker for worker in workers if worker.is_leader.is_set()]
    assert len(leaders) == 1

    for worker in workers:
        await worker.stop()


@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [
                Leadership(poll_interval=5, timeout=10),
            ],
            "no_default_plugins": True,
        }
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_leadership_transition(chancy: Chancy):
    """
    Ensures that leadership transitions work as expected.
    """
    worker1 = Worker(chancy, worker_id="1")
    worker2 = Worker(chancy, worker_id="2")

    await worker1.start()
    await worker2.start()

    await asyncio.sleep(15)

    # Only one worker should have become the leader.
    assert worker1.is_leader.is_set() != worker2.is_leader.is_set()

    # Who got the leadership.
    who_leads = worker1 if worker1.is_leader.is_set() else worker2
    who_follows = worker2 if worker1.is_leader.is_set() else worker1

    await who_leads.stop()

    await asyncio.sleep(15)

    assert who_follows.is_leader.is_set()
    await who_follows.stop()
