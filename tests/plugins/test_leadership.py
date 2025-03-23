import asyncio

import pytest

from chancy.app import Chancy
from chancy.worker import Worker
from chancy.plugins.leadership import ImmediateLeadership, Leadership


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
