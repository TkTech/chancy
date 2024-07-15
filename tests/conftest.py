import asyncio
import secrets
from typing import AsyncIterator

import pytest_asyncio
from pytest_postgresql import factories

from chancy import Chancy, Worker


def run_chancy_migrations(host, port, user, dbname, password):
    """
    Bootstraps the database with the required Chancy migrations.
    """
    import asyncio

    from chancy.app import Chancy

    async def main():
        async with Chancy(
            dsn=f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        ) as app:
            await app.migrate()

    asyncio.run(main())


external_postgres = factories.postgresql_noproc(
    host="localhost",
    password="localtest",
    user="postgres",
    port=8190,
    load=[run_chancy_migrations],
    dbname=f"chancy_test_{secrets.token_hex(8)}",
)
postgresql = factories.postgresql(
    "external_postgres",
)


@pytest_asyncio.fixture
async def chancy(request, postgresql):
    """
    Provides a Chancy application instance with an open connection pool
    to the test database.
    """
    i = postgresql.info

    async with Chancy(
        dsn=f"postgresql://{i.user}:{i.password}@{i.host}:{i.port}/{i.dbname}",
        **getattr(request, "param", {}),
    ) as chancy:
        await chancy.migrate()
        yield chancy
        await chancy.migrate(to_version=0)


@pytest_asyncio.fixture
async def worker(chancy) -> AsyncIterator[tuple[Worker, asyncio.Task]]:
    """
    Starts and returns a Worker and the task associated with it.

    If the worker is not stopped by the time the test completes, it will be
    cancelled.
    """
    worker = Worker(chancy)
    worker_task = asyncio.create_task(worker.start())

    try:
        yield worker, worker_task
    finally:
        if not worker_task.done():
            worker_task.cancel()

        try:
            await asyncio.gather(worker_task)
        except asyncio.CancelledError:
            pass


@pytest_asyncio.fixture
async def worker_no_start(chancy) -> Worker:
    """
    Returns a Worker instance that has not been started.
    """
    return Worker(chancy)
