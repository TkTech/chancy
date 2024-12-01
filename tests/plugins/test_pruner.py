import pytest

from chancy import Chancy, Queue, Job, QueuedJob, Worker
from chancy.plugins.pruner import Pruner
from chancy.plugins.leadership import ImmediateLeadership


def job_to_run():
    pass


@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [
                ImmediateLeadership(),
                Pruner(Pruner.Rules.Age() > 0, poll_interval=10),
            ]
        }
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_basic_pruning(chancy: Chancy, worker: Worker):
    """
    Test that completed jobs are pruned after aging.
    """
    await chancy.declare(Queue("default"))

    # Push and wait for job to complete
    ref = await chancy.push(Job.from_func(job_to_run))
    job = await chancy.wait_for_job(ref, timeout=30)
    assert job.state == QueuedJob.State.SUCCEEDED

    await worker.hub.wait_for("pruner.removed", timeout=30)

    with pytest.raises(KeyError):
        await chancy.get_job(ref)


@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [
                ImmediateLeadership(),
                Pruner(
                    Pruner.Rules.Age() > 0, maximum_to_prune=2, poll_interval=10
                ),
            ]
        }
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_maximum_prune_limit(chancy: Chancy, worker: Worker):
    """
    Test that pruner respects maximum_to_prune setting.
    """
    await chancy.declare(Queue("default"))

    # Create 5 jobs
    refs = []
    for _ in range(5):
        ref = await chancy.push(Job.from_func(job_to_run))
        job = await chancy.wait_for_job(ref, timeout=30)
        assert job.state == QueuedJob.State.SUCCEEDED
        refs.append(ref)

    await worker.hub.wait_for("pruner.removed", timeout=30)

    # Should have 3 jobs left (5 - maximum_to_prune)
    remaining = 0
    for ref in refs:
        try:
            await chancy.get_job(ref)
            remaining += 1
        except KeyError:
            pass

    assert remaining == 3


@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [
                ImmediateLeadership(),
                Pruner(
                    (Pruner.Rules.Age() > 0) & (Pruner.Rules.Queue() == "fast"),
                    poll_interval=10,
                ),
            ]
        }
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_queue_specific_pruning(chancy: Chancy, worker: Worker):
    """
    Test that pruning rules can target specific queues.
    """
    await chancy.declare(Queue("fast"))
    await chancy.declare(Queue("slow"))

    # Create job in fast queue
    fast_ref = await chancy.push(Job.from_func(job_to_run).with_queue("fast"))
    fast_job = await chancy.wait_for_job(fast_ref, timeout=30)
    assert fast_job.state == QueuedJob.State.SUCCEEDED

    # Create job in slow queue
    slow_ref = await chancy.push(Job.from_func(job_to_run).with_queue("slow"))
    slow_job = await chancy.wait_for_job(slow_ref, timeout=30)
    assert slow_job.state == QueuedJob.State.SUCCEEDED

    # Wait for pruning cycle
    await worker.hub.wait_for("pruner.removed", timeout=30)

    # Fast queue job should be pruned
    with pytest.raises(KeyError):
        await chancy.get_job(fast_ref)

    # Slow queue job should remain
    remaining_job = await chancy.get_job(slow_ref)
    assert remaining_job is not None


@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [
                ImmediateLeadership(),
                Pruner(Pruner.Rules.Age() > 0, poll_interval=10),
            ]
        }
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_preserve_pending_running(chancy: Chancy, worker: Worker):
    """
    Test that pending and running jobs are not pruned regardless of age.
    """
    await chancy.declare(
        Queue("default", concurrency=0)
    )  # Prevent job execution

    # Create a job that won't run due to concurrency=0
    ref = await chancy.push(Job.from_func(job_to_run))
    await worker.hub.wait_for("pruner.removed", timeout=30)

    # Job should still exist in pending state
    job = await chancy.get_job(ref)
    assert job.state == QueuedJob.State.PENDING
