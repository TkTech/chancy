import asyncio
from typing import Any

import pytest

from chancy import Chancy, Worker, Queue, QueuedJob, job
from chancy.plugin import Plugin


class RunCounter(Plugin):
    @staticmethod
    def get_identifier() -> str:
        return "run_counter"

    def __init__(self):
        super().__init__()
        self.runs = 0

    def on_job_completed(
        self,
        *,
        worker: "Worker",
        job: QueuedJob,
        exc: Exception | None = None,
        result: Any | None = None,
    ) -> QueuedJob:
        self.runs += 1
        return job


@job()
async def long_running_job():
    await asyncio.sleep(5)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chancy",
    [{"plugins": [RunCounter()]}],
    indirect=True
)
async def test_regression_51(chancy: Chancy, worker: Worker):
    """
    Test that pushing a job with a unique key while the same job is running
    does not result in concurrent execution of the job.

    Uses a shim plugin to count the # of jobs actually run, since the issue
    causes ID re-use, we can't rely on job IDs to count runs.
    """
    await chancy.declare(Queue("default"))
    # If working properly, ref_one and ref_two should be the same. However, if
    # we wait until ref_one is actively running, then ref_two will be allowed
    # to be pushed again, and we'll get a new reference ID.
    await chancy.push(long_running_job.job.with_unique_key("unique_long_running_job"))
    await asyncio.sleep(1)
    await chancy.push(long_running_job.job.with_unique_key("unique_long_running_job"))
    await asyncio.sleep(10)

    assert chancy.plugins[RunCounter.get_identifier()].runs == 1