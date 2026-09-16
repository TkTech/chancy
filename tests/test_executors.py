import signal
import sys
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from conftest import executor_params

from chancy import Chancy, Limit, Queue, Worker
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
        created_at=datetime.now(tz=UTC),
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


class InjectingMixin:
    @classmethod
    def get_function_and_kwargs(cls, job):
        func, kwargs = super().get_function_and_kwargs(job)
        return func, {**kwargs, "injected": f"by {cls.__name__}"}


class InjectingAsyncExecutor(InjectingMixin, AsyncExecutor):
    pass


class InjectingThreadedExecutor(InjectingMixin, ThreadedExecutor):
    pass


class InjectingProcessExecutor(InjectingMixin, ProcessExecutor):
    pass


if sys.version_info >= (3, 13):
    from chancy.executors.sub import SubInterpreterExecutor

    class InjectingSubInterpreterExecutor(
        InjectingMixin, SubInterpreterExecutor
    ):
        pass


def job_with_injected_kwarg(*, injected: str, context: QueuedJob):
    context.meta["injected"] = injected


async def async_job_with_injected_kwarg(*, injected: str, context: QueuedJob):
    context.meta["injected"] = injected


def test_get_function_and_kwargs_is_overridable():
    """
    Executor subclasses can inject keyword arguments into every job they
    run, and doing so keeps the job context detection working (#46).
    """
    job = queued_job(job_with_injected_kwarg)

    prepared, func, kwargs = (
        InjectingThreadedExecutor.prepare_job_for_execution(job)
    )

    assert func is job_with_injected_kwarg
    assert kwargs == {
        "injected": "by InjectingThreadedExecutor",
        "context": prepared,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("executor", executor_params())
async def test_injected_kwargs_reach_jobs_on_every_executor(
    chancy: Chancy, worker: Worker, executor: str
):
    """
    The override runs wherever the executor runs the job (event loop, pool
    thread, sub-interpreter or child process), so injected keyword arguments
    reach the job on every built-in executor (#46).
    """
    executor_class = import_string(executor)
    injecting_executor = f"{__name__}.Injecting{executor_class.__name__}"
    job_func = (
        job_with_injected_kwarg
        if executor_class.supports(Executor.Capability.SYNC_JOBS)
        else async_job_with_injected_kwarg
    )

    await chancy.declare(Queue("injected", executor=injecting_executor))
    ref = await chancy.push(Job.from_func(job_func, queue="injected"))
    finished = await chancy.wait_for_job(ref, timeout=30)

    assert finished.state == QueuedJob.State.SUCCEEDED
    assert finished.meta["injected"] == f"by Injecting{executor_class.__name__}"
