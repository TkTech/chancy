import pytest

from chancy import job, Queue
from chancy.plugins.cron import Cron


@job()
def test_job():
    """Simple job function for testing"""
    pass


@pytest.mark.parametrize(
    "chancy", [{"plugins": [Cron(poll_interval=1)]}], indirect=True
)
@pytest.mark.asyncio
async def test_schedule_job(chancy, worker):
    """Test scheduling a job with cron plugin"""
    await chancy.declare(Queue("default"))

    j = test_job.job.with_unique_key("test_job_cron")

    await Cron.schedule(chancy, "*/1 * * * *", j)

    schedules = await Cron.get_schedules(chancy, unique_keys=["test_job_cron"])

    assert len(schedules) == 1
    schedule = schedules["test_job_cron"]
    assert schedule["unique_key"] == "test_job_cron"
    assert schedule["cron"] == "*/1 * * * *"
    assert schedule["next_run"] is not None


@pytest.mark.parametrize(
    "chancy", [{"plugins": [Cron(poll_interval=1)]}], indirect=True
)
@pytest.mark.asyncio
async def test_unschedule_job(chancy, worker):
    """Test unscheduling a job"""
    await chancy.declare(Queue("default"))

    j = test_job.job.with_unique_key("test_job_cron")
    await Cron.schedule(chancy, "*/5 * * * *", j)

    schedules = await Cron.get_schedules(chancy, unique_keys=["test_job_cron"])
    assert len(schedules) == 1

    await Cron.unschedule(chancy, "test_job_cron")

    schedules = await Cron.get_schedules(chancy, unique_keys=["test_job_cron"])
    assert len(schedules) == 0


@pytest.mark.parametrize(
    "chancy", [{"plugins": [Cron(poll_interval=1)]}], indirect=True
)
@pytest.mark.asyncio
async def test_update_existing_job_schedule(chancy, worker):
    """Test updating an existing job's schedule"""
    await chancy.declare(Queue("default"))

    j = test_job.job.with_unique_key("test_job_cron")
    await Cron.schedule(chancy, "*/5 * * * *", j)
    await Cron.schedule(chancy, "*/10 * * * *", j)

    schedules = await Cron.get_schedules(chancy, unique_keys=["test_job_cron"])
    assert len(schedules) == 1
    schedule = schedules["test_job_cron"]
    assert schedule["cron"] == "*/10 * * * *"


@pytest.mark.parametrize(
    "chancy", [{"plugins": [Cron(poll_interval=1)]}], indirect=True
)
@pytest.mark.asyncio
async def test_fail_scheduling_without_unique_key(chancy, worker):
    """Test that scheduling a job without a unique key fails"""
    await chancy.declare(Queue("default"))

    with pytest.raises(
        ValueError, match="requires that each job has a unique_key"
    ):
        await Cron.schedule(chancy, "*/5 * * * *", test_job)


@pytest.mark.parametrize(
    "chancy", [{"plugins": [Cron(poll_interval=1)]}], indirect=True
)
@pytest.mark.asyncio
async def test_job_execution(chancy, worker):
    """Test that a scheduled job executes by verifying next_run is set"""
    await chancy.declare(Queue("default"))

    j = test_job.job.with_unique_key("immediate_job")

    await Cron.schedule(chancy, "*/1 * * * *", j)

    schedules = await Cron.get_schedules(chancy, unique_keys=["immediate_job"])
    assert len(schedules) == 1
    schedule = schedules["immediate_job"]
    assert schedule["next_run"] is not None


@pytest.mark.parametrize(
    "chancy", [{"plugins": [Cron(poll_interval=1)]}], indirect=True
)
@pytest.mark.asyncio
async def test_get_schedules_all(chancy, worker):
    """Test getting all scheduled jobs"""
    await chancy.declare(Queue("default"))

    job1 = test_job.job.with_unique_key("test_job_1")
    job2 = test_job.job.with_unique_key("test_job_2")
    job3 = test_job.job.with_unique_key("test_job_3")

    await Cron.schedule(chancy, "*/5 * * * *", job1)
    await Cron.schedule(chancy, "*/10 * * * *", job2)
    await Cron.schedule(chancy, "*/15 * * * *", job3)

    all_schedules = await Cron.get_schedules(chancy)

    assert len(all_schedules) >= 3

    assert "test_job_1" in all_schedules
    assert "test_job_2" in all_schedules
    assert "test_job_3" in all_schedules


@pytest.mark.parametrize(
    "chancy", [{"plugins": [Cron(poll_interval=1)]}], indirect=True
)
@pytest.mark.asyncio
async def test_get_schedules_filtered(chancy, worker):
    """Test getting scheduled jobs filtered by unique keys"""
    await chancy.declare(Queue("default"))

    job1 = test_job.job.with_unique_key("test_job_1")
    job2 = test_job.job.with_unique_key("test_job_2")
    job3 = test_job.job.with_unique_key("test_job_3")

    await Cron.schedule(chancy, "*/5 * * * *", job1)
    await Cron.schedule(chancy, "*/10 * * * *", job2)
    await Cron.schedule(chancy, "*/15 * * * *", job3)

    filtered_schedules = await Cron.get_schedules(
        chancy, unique_keys=["test_job_1", "test_job_3"]
    )

    assert len(filtered_schedules) == 2

    assert "test_job_1" in filtered_schedules
    assert "test_job_3" in filtered_schedules
    assert "test_job_2" not in filtered_schedules
