import asyncio
import json
from datetime import UTC
from zoneinfo import ZoneInfo

import pytest
from psycopg import sql

from chancy import Queue, job
from chancy.plugins.cron import Cron

PARIS = ZoneInfo("Europe/Paris")


@job()
def test_job():
    """Simple job function for testing"""


@pytest.mark.parametrize(
    "chancy",
    [{"plugins": [Cron(poll_interval=1)], "no_default_plugins": True}],
    indirect=True,
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
    "chancy",
    [{"plugins": [Cron(poll_interval=1)], "no_default_plugins": True}],
    indirect=True,
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
    "chancy",
    [{"plugins": [Cron(poll_interval=1)], "no_default_plugins": True}],
    indirect=True,
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
    "chancy",
    [{"plugins": [Cron(poll_interval=1)], "no_default_plugins": True}],
    indirect=True,
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
    "chancy",
    [{"plugins": [Cron(poll_interval=1)], "no_default_plugins": True}],
    indirect=True,
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
    "chancy",
    [{"plugins": [Cron(poll_interval=1)], "no_default_plugins": True}],
    indirect=True,
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
    "chancy",
    [{"plugins": [Cron(poll_interval=1)], "no_default_plugins": True}],
    indirect=True,
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


@pytest.mark.parametrize(
    "chancy",
    [{"plugins": [Cron(poll_interval=1)], "no_default_plugins": True}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_schedule_with_timezone(chancy, worker):
    """Test that a schedule's timezone is stored and used for next_run"""
    await chancy.declare(Queue("default"))

    j = test_job.job.with_unique_key("test_job_paris")
    await Cron.schedule(chancy, "0 9 * * *", j, timezone=PARIS)

    schedule = (await Cron.get_schedules(chancy))["test_job_paris"]
    assert schedule["timezone"] == "Europe/Paris"

    next_run = schedule["next_run"].astimezone(PARIS)
    assert (next_run.hour, next_run.minute) == (9, 0)


@pytest.mark.parametrize(
    "chancy",
    [{"plugins": [Cron(poll_interval=1)], "no_default_plugins": True}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_schedule_without_timezone_is_utc(chancy, worker):
    """Test that a schedule without a timezone is evaluated in UTC"""
    await chancy.declare(Queue("default"))

    j = test_job.job.with_unique_key("test_job_utc")
    await Cron.schedule(chancy, "0 9 * * *", j)

    schedule = (await Cron.get_schedules(chancy))["test_job_utc"]
    assert schedule["timezone"] == "Etc/UTC"

    next_run = schedule["next_run"].astimezone(UTC)
    assert (next_run.hour, next_run.minute) == (9, 0)


@pytest.mark.parametrize(
    "chancy",
    [{"plugins": [Cron(poll_interval=1)], "no_default_plugins": True}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_update_existing_job_timezone(chancy, worker):
    """Test that rescheduling a job updates its timezone"""
    await chancy.declare(Queue("default"))

    j = test_job.job.with_unique_key("test_job_cron")
    await Cron.schedule(chancy, "0 9 * * *", j, timezone=PARIS)
    await Cron.schedule(chancy, "0 9 * * *", j)

    schedule = (await Cron.get_schedules(chancy))["test_job_cron"]
    assert schedule["timezone"] == "Etc/UTC"


@pytest.mark.parametrize(
    "chancy",
    [{"plugins": [Cron(poll_interval=1)], "no_default_plugins": True}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_existing_schedules_default_to_utc(chancy):
    """Test that schedules created before the timezone column become UTC"""
    plugin = chancy.plugins[Cron.get_identifier()]
    await plugin.migrate(chancy, to_version=2)

    async with chancy.pool.connection() as conn:
        await conn.execute(
            sql.SQL(
                """
                INSERT INTO {table} (unique_key, job, cron, next_run)
                VALUES ('legacy', %s, '0 9 * * *', NOW())
                """
            ).format(table=sql.Identifier(f"{chancy.prefix}cron")),
            [json.dumps(test_job.job.with_unique_key("legacy").pack())],
        )

    await plugin.migrate(chancy)

    schedule = (await Cron.get_schedules(chancy))["legacy"]
    assert schedule["timezone"] == "Etc/UTC"


@pytest.mark.parametrize(
    "chancy",
    [{"plugins": [Cron(poll_interval=1)], "no_default_plugins": True}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_schedules_with_different_timezones_run(chancy, worker):
    """
    Test that the polling loop runs due schedules in different timezones and
    reschedules each one at 09:00 in its own timezone.
    """
    await chancy.declare(Queue("default"))

    zones = {
        "job_utc": ZoneInfo("Etc/UTC"),
        "job_paris": PARIS,
        "job_tokyo": ZoneInfo("Asia/Tokyo"),
    }
    for key, tz in zones.items():
        j = test_job.job.with_unique_key(key)
        await Cron.schedule(chancy, "0 9 * * *", j, timezone=tz)

    table = sql.Identifier(f"{chancy.prefix}cron")
    async with chancy.pool.connection() as conn:
        # Make every schedule due now.
        await conn.execute(
            sql.SQL(
                "UPDATE {table} SET next_run = NOW() - INTERVAL '1 minute'"
            ).format(table=table)
        )

    for _ in range(20):
        schedules = await Cron.get_schedules(chancy)
        if all(schedules[key]["last_run"] for key in zones):
            break
        await asyncio.sleep(0.5)
    else:
        pytest.fail("The cron plugin did not run every due schedule.")

    async with chancy.pool.connection() as conn:
        cursor = await conn.execute(
            sql.SQL("SELECT unique_key FROM {jobs}").format(
                jobs=sql.Identifier(f"{chancy.prefix}jobs")
            )
        )
        pushed = {row[0] for row in await cursor.fetchall()}
    assert pushed >= zones.keys()

    for key, tz in zones.items():
        schedule = schedules[key]
        assert schedule["timezone"] == tz.key
        next_run = schedule["next_run"].astimezone(tz)
        assert (next_run.hour, next_run.minute) == (9, 0)
        assert schedule["next_run"] > schedule["last_run"]

    # 09:00 in each zone is a different instant.
    assert len({schedules[key]["next_run"] for key in zones}) == 3
