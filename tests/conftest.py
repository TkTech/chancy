import asyncio
import os
import sys
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

from chancy import Chancy, Worker
from chancy.executors.base import Executor
from chancy.utils import import_string


def executor_params(
    required: Executor.Capability = Executor.Capability.NONE,
    *,
    excluded: Executor.Capability = Executor.Capability.NONE,
):
    """Return built-in executors matching the requested capabilities."""
    params = []
    for executor in Chancy.Executor:
        try:
            executor_class = import_string(executor)
        except ImportError:
            if (
                executor is Chancy.Executor.SubInterpreter
                and sys.version_info < (3, 13)
            ):
                continue
            raise

        capabilities = executor_class.get_capabilities()
        if (capabilities & required) != required:
            continue
        if capabilities & excluded:
            continue

        params.append(pytest.param(executor, id=executor.name.lower()))

    return params


@pytest.fixture(scope="session")
def xdist_worker() -> str | None:
    """Returns the xdist worker ID (e.g., 'gw0') or None if not running parallel."""
    return os.environ.get("PYTEST_XDIST_WORKER")


@pytest.fixture(scope="session")
def test_prefix(xdist_worker) -> str:
    """
    Unique table prefix for parallel test isolation.

    Returns 'chancy_' normally, or 'chancy_gw0_' etc. when running with xdist.
    """
    if xdist_worker is None:
        return "chancy_"
    return f"chancy_{xdist_worker}_"


@pytest.fixture(scope="session")
def test_suffix(xdist_worker) -> str:
    """
    Unique suffix for test resources (tables, schemas) that need isolation.

    Returns '' normally, or '_gw0' etc. when running with xdist.
    """
    if xdist_worker is None:
        return ""
    return f"_{xdist_worker}"


def pytest_asyncio_loop_factories(config, item):
    # Since psycopg's asyncio implementation cannot use the default
    # proactor event loop on Windows, we need to use a selector event loop.
    # SelectorEventLoop is already the default everywhere else.
    return {"selector": asyncio.SelectorEventLoop}


@pytest_asyncio.fixture()
async def chancy(request, test_prefix):
    """
    Provides a Chancy application instance with an open connection pool
    to the test database.

    When running with pytest-xdist, each worker gets a unique table prefix
    to enable parallel test execution without conflicts.
    """
    params = getattr(request, "param", {})
    # Allow tests to override prefix, but default to worker-specific prefix
    if "prefix" not in params:
        params = {**params, "prefix": test_prefix}

    async with Chancy(
        "postgresql://postgres:localtest@localhost:8190/postgres",
        **params,
    ) as chancy:
        await chancy.migrate()
        yield chancy
        await chancy.migrate(to_version=0)


@pytest.fixture
def chancy_just_app(test_prefix):
    """
    Provides just a configured chancy instance with no open connection pool
    or migrations.

    When running with pytest-xdist, uses a unique table prefix for isolation.
    """
    return Chancy(
        "postgresql://postgres:localtest@localhost:8190/postgres",
        prefix=test_prefix,
    )


@pytest_asyncio.fixture()
async def worker(request, chancy) -> AsyncIterator[Worker]:
    """
    Starts and returns a Worker and the task associated with it.

    If the worker is not stopped by the time the test completes, it will be
    cancelled.
    """
    async with Worker(
        chancy, shutdown_timeout=60, **getattr(request, "param", {})
    ) as worker:
        yield worker


@pytest_asyncio.fixture()
async def worker_no_start(chancy) -> Worker:
    """
    Returns a Worker instance that has not been started.
    """
    return Worker(chancy)


@pytest.fixture(params=executor_params(Executor.Capability.SYNC_JOBS))
def sync_job_executor(request):
    """Provide each executor that supports synchronous jobs."""
    return request.param


@pytest.fixture(params=executor_params(Executor.Capability.ASYNC_JOBS))
def async_job_executor(request):
    """Provide each executor that supports asynchronous jobs."""
    return request.param


@pytest.fixture(
    params=executor_params(Executor.Capability.COOPERATIVE_TIME_LIMITS)
)
def cooperative_time_limit_executor(request):
    """Provide each executor with cooperative time limits."""
    return request.param


@pytest.fixture(
    params=executor_params(Executor.Capability.AUTOMATIC_TIME_LIMITS)
)
def automatic_time_limit_executor(request):
    """Provide each executor with automatic time limits."""
    return request.param


@pytest.fixture(params=executor_params(Executor.Capability.CANCELLATION))
def cancellation_executor(request):
    """Provide each executor that can cancel an active job."""
    return request.param


@pytest.fixture(
    params=executor_params(excluded=Executor.Capability.MEMORY_LIMITS)
)
def executor_without_memory_limits(request):
    """Provide each executor without memory limit support."""
    return request.param


@pytest.fixture(params=executor_params(excluded=Executor.Capability.SYNC_JOBS))
def executor_without_sync_jobs(request):
    """Provide each executor without synchronous job support."""
    return request.param
