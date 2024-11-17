from datetime import datetime, timezone

import pytest
from psycopg import sql

from chancy import Chancy, Job, Queue, Worker
from chancy.utils import chancy_uuid, timed_block


def dummy_job():
    pass


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chancy",
    [
        {
            "notifications": False,
        }
    ],
    indirect=True,
)
async def test_busy_chancy(chancy: Chancy, worker_no_start: Worker):
    """
    Tests the performance of the worker when the Chancy instance has 10 million
    completed jobs and 40,000 pending jobs.

    This is NOT testing optimal cases - it's here to catch egregious performance
    regressions. For example our first test checks job fetches are under 100ms,
    but it's typically sub 1ms in reality.
    """
    queue = Queue("default", concurrency=10)
    await chancy.declare(queue)

    async with chancy.pool.connection() as conn:
        async with conn.cursor() as cursor:
            async with cursor.copy(
                sql.SQL(
                    "COPY {jobs_table} ("
                    "   id,"
                    "   queue,"
                    "   state,"
                    "   func,"
                    "   kwargs,"
                    "   meta,"
                    "   created_at,"
                    "   scheduled_at"
                    ")"
                    " FROM STDIN"
                ).format(jobs_table=sql.Identifier(f"{chancy.prefix}jobs"))
            ) as copy:
                for i in range(10_000_000):
                    await copy.write_row(
                        (
                            chancy_uuid(),
                            "default",
                            "succeeded",
                            "dummy_job",
                            "{}",
                            "{}",
                            datetime.now(timezone.utc),
                            datetime.now(timezone.utc),
                        )
                    )

                j = Job.from_func(dummy_job)

                for i in range(40_000):
                    await copy.write_row(
                        (
                            chancy_uuid(),
                            "default",
                            "pending",
                            j.func,
                            "{}",
                            "{}",
                            datetime.now(timezone.utc),
                            datetime.now(timezone.utc),
                        )
                    )

            await conn.commit()

        with timed_block() as timer:
            await worker_no_start.fetch_jobs(queue, conn, up_to=1)
        assert timer.elapsed < 0.1

        with timed_block() as timer:
            await worker_no_start.fetch_jobs(queue, conn, up_to=100)
        assert timer.elapsed < 0.1
