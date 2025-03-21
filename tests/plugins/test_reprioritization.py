import asyncio
import datetime

import pytest

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
