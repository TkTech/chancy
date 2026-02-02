import asyncio
import os
from typing import AsyncIterator

import pytest
import pytest_asyncio
import sys

from chancy import Chancy, Worker


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


@pytest.fixture(scope="session")
def event_loop_policy():
    # Since psycopg's asyncio implementation cannot use the default
    # proactor event loop on Windows, we need to use the selector event loop.
    if sys.platform == "win32":
        return asyncio.WindowsSelectorEventLoopPolicy()
    return asyncio.DefaultEventLoopPolicy()


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


@pytest.fixture(
    params=(
        [Chancy.Executor.Process, Chancy.Executor.Threaded]
        + (
            [Chancy.Executor.SubInterpreter]
            if sys.version_info >= (3, 13)
            else []
        )
    )
)
def sync_executor(request):
    """
    Provides a parameterized fixture for all sync executors.
    """
    return request.param


@pytest.fixture(params=[Chancy.Executor.Async])
def async_executor(request):
    """
    Provides a parameterized fixture for all async executors.
    """
    return request.param
