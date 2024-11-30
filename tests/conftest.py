import asyncio
import secrets
from typing import AsyncIterator

import pytest
import pytest_asyncio
import sys
from pytest_postgresql import factories

from chancy import Chancy, Worker


external_postgres = factories.postgresql_noproc(
    host="localhost",
    password="localtest",
    user="postgres",
    port=8190,
    dbname=f"chancy_test_{secrets.token_hex(8)}",
)
postgresql = factories.postgresql(
    "external_postgres",
)


@pytest_asyncio.fixture()
async def chancy(request, postgresql):
    """
    Provides a Chancy application instance with an open connection pool
    to the test database.
    """
    i = postgresql.info

    async with Chancy(
        f"postgresql://{i.user}:{i.password}@{i.host}:{i.port}/{i.dbname}",
        **getattr(request, "param", {}),
    ) as chancy:
        await chancy.migrate()
        yield chancy
        await chancy.migrate(to_version=0)


@pytest.fixture
def chancy_just_app(postgresql):
    """
    Provides just a configured chancy instance with no open connection pool
    or migrations.
    """
    i = postgresql.info
    return Chancy(
        f"postgresql://{i.user}:{i.password}@{i.host}:{i.port}/{i.dbname}",
    )


@pytest_asyncio.fixture()
async def worker(request, chancy) -> AsyncIterator[tuple[Worker, asyncio.Task]]:
    """
    Starts and returns a Worker and the task associated with it.

    If the worker is not stopped by the time the test completes, it will be
    cancelled.
    """
    async with Worker(chancy, **getattr(request, "param", {})) as worker:
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
