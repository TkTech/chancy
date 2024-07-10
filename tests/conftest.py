import asyncio
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
)
postgresql = factories.postgresql(
    "external_postgres",
)


@pytest_asyncio.fixture
async def chancy(postgresql):
    """
    Provides a Chancy application instance with an open connection pool
    to the test database.
    """
    i = postgresql.info

    async with Chancy(
        dsn=f"postgresql://{i.user}:{i.password}@{i.host}:{i.port}/{i.dbname}",
        plugins=[],
    ) as chancy:
        yield chancy


@pytest_asyncio.fixture
async def worker(chancy) -> AsyncIterator[tuple[Worker, asyncio.Task]]:
    """
    Starts and returns a Worker and the task associated with it.

    If the worker is not stopped by the time the test completes, it will be
    cancelled.
    """
    async with asyncio.TaskGroup() as tg:
        worker = Worker(chancy)
        worker_task = tg.create_task(worker.start())
        try:
            yield worker, worker_task
        finally:
            if not worker_task.done():
                worker_task.cancel()
