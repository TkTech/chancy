import signal
import sys
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from chancy import Limit
from chancy.executors.asyncex import AsyncExecutor
from chancy.executors.base import Executor
from chancy.executors.process import ProcessExecutor, resource
from chancy.executors.thread import ThreadedExecutor
from chancy.job import Job, QueuedJob
from chancy.utils import import_string


def queued_job(func, *, limits=None):
    return QueuedJob(
        func=Job.from_func(func).func,
        id=uuid4(),
        created_at=datetime.now(tz=timezone.utc),
        limits=limits or [],
    )


def test_builtin_executor_capabilities():
    capability = Executor.Capability

    assert ThreadedExecutor.get_capabilities() == (
        capability.SYNC_JOBS
        | capability.ASYNC_JOBS
        | capability.COOPERATIVE_TIME_LIMITS
    )
    assert AsyncExecutor.get_capabilities() == (
        capability.ASYNC_JOBS
        | capability.CANCELLATION
        | capability.AUTOMATIC_TIME_LIMITS
    )
    if sys.version_info >= (3, 13):
        from chancy.executors.sub import SubInterpreterExecutor

        assert SubInterpreterExecutor.get_capabilities() == (
            capability.SYNC_JOBS
            | capability.ASYNC_JOBS
            | capability.COOPERATIVE_TIME_LIMITS
        )

    assert ProcessExecutor.supports(capability.SYNC_JOBS)
    assert ProcessExecutor.supports(capability.ASYNC_JOBS)
    assert ProcessExecutor.supports(capability.CANCELLATION) == hasattr(
        signal, "SIGUSR1"
    )
    assert ProcessExecutor.supports(
        capability.AUTOMATIC_TIME_LIMITS
    ) == hasattr(signal, "SIGALRM")
    assert ProcessExecutor.supports(capability.MEMORY_LIMITS) == (
        resource is not None and hasattr(resource, "RLIMIT_AS")
    )


def test_unsupported_memory_limits_are_rejected(
    executor_without_memory_limits,
):
    executor = import_string(executor_without_memory_limits)
    job = queued_job(
        len,
        limits=[Limit(Limit.Type.MEMORY, 1024)],
    )

    with pytest.raises(ValueError, match="does not support memory limits"):
        executor.prepare_job_for_execution(job)


def test_unsupported_function_type_is_rejected(
    executor_without_sync_jobs,
):
    executor = import_string(executor_without_sync_jobs)
    job = queued_job(len)

    with pytest.raises(ValueError, match="does not support synchronous jobs"):
        executor.prepare_job_for_execution(job)
