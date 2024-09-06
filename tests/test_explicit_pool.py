import pytest

from chancy import Chancy, JobInstance, Job, Queue


def job_to_run():
    return


@pytest.mark.asyncio
async def test_basic_job(chancy_just_app: Chancy):
    """
    Simply test that we can push a job, and it runs successfully.
    """
    await chancy_just_app.migrate()
    await chancy_just_app.declare(Queue("default"))
    ref = await chancy_just_app.push(Job.from_func(job_to_run))
    job = await chancy_just_app.get_job(ref)
    assert job.state == JobInstance.State.PENDING
    await chancy_just_app.migrate(to_version=0)
    await chancy_just_app.pool.close()
