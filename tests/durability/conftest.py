import asyncio
import logging
from collections.abc import AsyncIterator, Iterator
from contextlib import AsyncExitStack
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import DictRow, dict_row

from chancy import Chancy, Queue, Worker
from chancy.plugin import Plugin
from chancy.plugins.recovery import Recovery
from chancy.plugins.workflow import WorkflowPlugin
from tests.durability import jobs
from tests.durability.helpers import Proxy


class Cluster:
    def __init__(
        self, app: Chancy, client: httpx.AsyncClient, stack: AsyncExitStack
    ):
        self.app = app
        self.client = client
        self.stack = stack
        self.workers: list[Worker] = []
        self.proxies: dict[str, Proxy] = {}

    async def start_worker(
        self,
        name: str = "a",
        *,
        leadership_timeout: int = 3,
        leadership_idle_timeout: int | None = 2,
        heartbeat_timeout: int = 8,
        backoff_max_retries: int | None = None,
    ) -> Worker:
        port = 18191 + len(self.workers)
        if port > 18192 or name in self.proxies:
            raise ValueError("Use at most two workers with distinct names")
        proxy = Proxy(self.client, f"{self.app.prefix}{name}")
        await proxy.create(port)
        self.proxies[name] = proxy
        self.stack.push_async_callback(proxy.delete)
        app = await self.stack.enter_async_context(
            Chancy(
                make_conninfo(
                    self.app.dsn,
                    port=port,
                    application_name=f"{self.app.prefix}{name}",
                ),
                prefix=self.app.prefix,
                plugins=plugins(),
                no_default_plugins=True,
            )
        )
        app.pool.timeout = 2
        worker = Worker(
            app,
            worker_id=name,
            register_signal_handlers=False,
            leadership_poll_interval=1,
            leadership_timeout=leadership_timeout,
            leadership_idle_timeout=leadership_idle_timeout,
            heartbeat_poll_interval=1,
            heartbeat_timeout=heartbeat_timeout,
            queue_change_poll_interval=1,
            backoff_initial=0.1,
            backoff_max=0.5,
            backoff_max_retries=backoff_max_retries,
            shutdown_timeout=10,
        )
        self.workers.append(worker)
        self.stack.push_async_callback(self.stop_worker, worker)
        await worker.start()
        return worker

    @staticmethod
    async def stop_worker(worker: Worker) -> None:
        async with asyncio.timeout(15):
            if not worker._finalized:
                await worker.stop()
            assert not list(worker.manager)

    async def rows(self, table: str) -> list[DictRow]:
        async with (
            self.app.pool.connection() as conn,
            conn.cursor(row_factory=dict_row) as cursor,
        ):
            await cursor.execute(
                sql.SQL("SELECT * FROM {}").format(
                    sql.Identifier(f"{self.app.prefix}{table}")
                )
            )
            return await cursor.fetchall()

    async def diagnostics(self) -> None:
        log = logging.getLogger(__name__)
        for worker in self.workers:
            log.info(
                "Worker %s: healthy=%s leader=%s tasks=%s",
                worker.worker_id,
                worker.healthy,
                worker.is_leader,
                [repr(task) for task in worker.manager],
            )
        for table in ("leader", "workers", "jobs"):
            log.info("%s: %s", table, await self.rows(table))
        async with self.app.pool.connection() as conn:
            cursor = await conn.execute(
                "SELECT pid, application_name, state, wait_event_type, "
                "wait_event, query FROM pg_stat_activity "
                "WHERE starts_with(application_name, %s)",
                [self.app.prefix],
            )
            log.info("Database activity: %s", await cursor.fetchall())
        response = await self.client.get("/proxies")
        log.info("Proxies: %s", response.json())


def plugins() -> list[Plugin]:
    return [Recovery(poll_interval=1), WorkflowPlugin(polling_interval=1)]


@pytest.fixture
def first_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[jobs.FirstExecutionGate]:
    gate = jobs.FirstExecutionGate()
    monkeypatch.setattr(jobs, "first_execution", gate)
    try:
        yield gate
    finally:
        gate.release.set()


@pytest_asyncio.fixture
async def cluster(
    request: pytest.FixtureRequest, first_execution: jobs.FirstExecutionGate
) -> AsyncIterator[Cluster]:
    if not request.config.getoption("--run-durability"):
        pytest.fail("Use --run-durability to run tests/durability")
    if hasattr(request.config, "workerinput") or getattr(
        request.config.option, "numprocesses", 0
    ):
        pytest.fail("Durability tests share proxy ports; run with -n 0")

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:18474", timeout=5, trust_env=False
    ) as client:
        try:
            (await client.get("/version")).raise_for_status()
        except httpx.HTTPError as exc:
            pytest.fail(
                "Start the durability stack with: docker compose "
                f"-f docker-compose.durability.yml up -d --wait ({exc})"
            )
        async with Chancy(
            "postgresql://postgres:localtest@127.0.0.1:18190/postgres"
            "?connect_timeout=2",
            prefix=f"dur_{uuid4().hex[:12]}_",
            plugins=plugins(),
            no_default_plugins=True,
        ) as app:
            app.pool.timeout = 5
            await app.migrate()
            try:
                await app.declare(
                    Queue(
                        "default",
                        executor=Chancy.Executor.Async,
                        concurrency=2,
                        polling_interval=1,
                    )
                )
                async with AsyncExitStack() as stack:
                    cluster = Cluster(app, client, stack)
                    try:
                        yield cluster
                    finally:
                        try:
                            async with asyncio.timeout(5):
                                await cluster.diagnostics()
                        except Exception:
                            logging.getLogger(__name__).exception(
                                "Failed to collect durability diagnostics"
                            )
                        finally:
                            for proxy in cluster.proxies.values():
                                await proxy.restore()
            finally:
                await app.migrate(to_version=0)
                async with app.pool.connection() as conn:
                    await conn.execute(
                        sql.SQL("DROP TABLE {}").format(
                            sql.Identifier(f"{app.prefix}schema_version")
                        )
                    )
