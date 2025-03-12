import asyncio
from typing import AsyncIterator

import pytest
import pytest_asyncio
import sys

from chancy import Chancy, Worker


@pytest.fixture(scope="session")
def event_loop_policy():
    # Since psycopg's asyncio implementation cannot use the default
    # proactor event loop on Windows, we need to use the selector event loop.
    if sys.platform == "win32":
        return asyncio.WindowsSelectorEventLoopPolicy()
    return asyncio.DefaultEventLoopPolicy()


@pytest_asyncio.fixture()
async def chancy(request):
    """
    Provides a Chancy application instance with an open connection pool
    to the test database.
    """
    async with Chancy(
        "postgresql://postgres:localtest@localhost:8190/postgres",
        **getattr(request, "param", {}),
    ) as chancy:
        await chancy.migrate()
        yield chancy
        await chancy.migrate(to_version=0)


@pytest.fixture
def chancy_just_app():
    """
    Provides just a configured chancy instance with no open connection pool
    or migrations.
    """
    return Chancy(
        "postgresql://postgres:localtest@localhost:8190/postgres",
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
