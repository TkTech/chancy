"""Run a busy dashboard from the repository root:

    uv run --extra web --extra cron python -m examples.demo
    uv run --extra web --extra cron python -m examples.demo --duration 600

Build dashboard assets first if needed: npm --prefix chancy/plugins/api/ui ci
then npm --prefix chancy/plugins/api/ui run build.

Uses the local docker-compose PostgreSQL by default; override with --dsn or
CHANCY_DEMO_DSN. All demo tables use the chancy_demo_ prefix. State survives
restarts; completed jobs and workflows are retained for two hours and one hour,
respectively. Run only one demo per database. Ctrl+C stops both workers.
Queue settings are restored on startup.

Open http://127.0.0.1:8000 and sign in with demo / demo. Failed jobs are
intentional. Metrics accumulate in real time; allow several minutes for charts.
Async, threaded, and process executors are exercised, along with worker tags,
priorities, rate limits, delayed jobs, unique keys, timeouts, retries, workflows,
cron, database triggers, pruning, leadership, recovery, and metrics. Recovery
is enabled but worker crashes are not deliberately induced.
"""

import argparse
import asyncio
import os
import random
import signal
import sys
from contextlib import AsyncExitStack
from datetime import UTC, datetime, timedelta
from uuid import uuid4
from zoneinfo import ZoneInfo

import uvicorn
from psycopg import sql

from chancy import Chancy, Job, Limit, Queue, Worker
from chancy.plugins.api import Api, SimpleAuthBackend
from chancy.plugins.cron import Cron
from chancy.plugins.leadership import Leadership
from chancy.plugins.metrics import Metrics
from chancy.plugins.pruner import Pruner
from chancy.plugins.reprioritize import Reprioritize
from chancy.plugins.retry import RetryPlugin
from chancy.plugins.trigger import Trigger
from chancy.plugins.workflow import Workflow, WorkflowPlugin
from examples import demo_jobs as jobs

PREFIX = "chancy_demo_"
BACKLOG = 500


def application(dsn):
    plugins = [
        Leadership(poll_interval=5, timeout=15),
        Cron(poll_interval=5),
        RetryPlugin(),
        Trigger(),
        Metrics(sync_interval=5, collection_interval=30),
        Pruner(rule=Pruner.Rules.Age() > 7200),
        WorkflowPlugin(pruning_rule=WorkflowPlugin.Rules.Age() > 3600),
        Reprioritize(
            (Reprioritize.Rules.Queue() == "reports")
            & (Reprioritize.Rules.Age() > 30),
            check_interval=30,
        ),
    ]
    return Chancy(dsn, prefix=PREFIX, plugins=plugins)


def task(func, queue, **kwargs):
    return Job.from_func(
        func,
        queue=queue,
        kwargs=kwargs,
        max_attempts=3,
        meta={
            "source": "dashboard-demo",
            "retry_settings": {"backoff": 3, "backoff_jitter": [0, 0]},
        },
    )


def catalogue(number, run_id):
    workflow = Workflow("Import catalogue")
    for stage, dependencies in [
        ("download", []),
        ("validate", ["download"]),
        ("thumbnails", ["download"]),
        ("publish", ["validate", "thumbnails"]),
    ]:
        workflow.add(
            stage,
            task(
                jobs.import_catalogue,
                "imports",
                catalogue=f"CAT-{run_id}-{number:04d}",
                stage=stage,
                seconds=1 + number % 3,
                fail_until=3 if number % 10 == 0 and stage == "validate" else 0,
            ),
            dependencies=dependencies,
        )
    return workflow


async def prepare(app):
    for name, options in [
        ("default", {}),
        ("orders", {"concurrency": 4}),
        ("notifications", {"rate_limit": 8, "rate_limit_window": 10}),
        ("imports", {"concurrency": 6}),
        (
            "reports",
            {"executor": Chancy.Executor.Threaded, "tags": {"reports"}},
        ),
        ("maintenance", {"executor": Chancy.Executor.Process}),
        ("paused", {"state": Queue.State.PAUSED}),
    ]:
        await app.declare(
            Queue(
                name,
                **(
                    {
                        "executor": Chancy.Executor.Async,
                        "concurrency": 1,
                        "tags": {"general"},
                    }
                    | options
                ),
            ),
            upsert=True,
        )
    await Cron.schedule(
        app,
        "* * * * *",
        task(jobs.reconcile_inventory, "maintenance").with_unique_key(
            "inventory"
        ),
    )
    await Cron.schedule(
        app,
        "0 9 * * 1-5",
        task(
            jobs.build_report, "reports", report="Daily sales"
        ).with_unique_key("daily-sales"),
        timezone=ZoneInfo("America/Montreal"),
    )
    # A single-row application table demonstrates triggers without growing.
    async with app.pool.connection() as conn:
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS chancy_demo_inventory "
            "(id integer PRIMARY KEY, updated_at timestamptz NOT NULL)"
        )
        await conn.execute(
            "INSERT INTO chancy_demo_inventory VALUES (1, now()) "
            "ON CONFLICT DO NOTHING"
        )
    triggers = await Trigger.get_triggers(app)
    if not any(
        t.table_name == "chancy_demo_inventory" for t in triggers.values()
    ):
        await Trigger.register_trigger(
            app,
            table_name="chancy_demo_inventory",
            operations=["UPDATE"],
            job_template=task(
                jobs.handle_inventory_change, "orders"
            ).with_unique_key("inventory-change"),
        )
    async for _ in app.push_many(
        [
            task(
                jobs.process_order, "paused", order=f"ON-HOLD-{i}"
            ).with_unique_key(f"on-hold-{i}")
            for i in range(5)
        ]
    ):
        pass


async def produce(app, seed, duration):
    rng = random.Random(seed)
    run_id = uuid4().hex[:8]
    loop = asyncio.get_running_loop()
    deadline = loop.time() + duration if duration else float("inf")
    batch = 0
    while loop.time() < deadline:
        async with app.pool.connection() as conn:
            cursor = await conn.execute(
                sql.SQL("""
                    SELECT count(*) FROM (
                        SELECT 1 FROM {jobs}
                        WHERE state IN ('pending', 'running', 'retrying')
                        LIMIT %s
                    ) AS outstanding
                """).format(jobs=sql.Identifier(f"{PREFIX}jobs")),
                (BACKLOG,),
            )
            outstanding = (await cursor.fetchone())[0]
        if outstanding < BACKLOG - 20:
            now = datetime.now(UTC)
            orders = [
                task(
                    jobs.process_order,
                    "orders",
                    order=f"ORDER-{run_id}-{batch:05d}-{i}",
                    seconds=rng.uniform(0.2, 3),
                )
                .with_priority(rng.choice([0, 0, 5, 10]))
                .with_scheduled_at(now + timedelta(seconds=30 if i == 0 else 0))
                for i in range(4)
            ]
            orders.append(
                task(
                    jobs.send_notification,
                    "notifications",
                    order=f"ORDER-{run_id}-{batch:05d}",
                    fail_until={0: 3, 1: 1, 2: 2}.get(batch % 10, 0),
                )
            )
            if batch % 5 == 0:
                orders.append(
                    task(
                        jobs.build_report,
                        "reports",
                        report=f"Sales {run_id}-{batch}",
                        seconds=12 if batch % 50 == 0 else 6,
                    )
                    .with_limits([Limit(Limit.Type.TIME, 10)])
                    .with_max_attempts(1)
                )
                workflow = catalogue(batch // 5, run_id)
                await WorkflowPlugin.push(app, workflow)
                if batch == 0:
                    print(
                        f"Example workflow: /workflows/{workflow.id}",
                        flush=True,
                    )
                async with app.pool.connection() as conn:
                    await conn.execute(
                        "UPDATE chancy_demo_inventory SET updated_at = now()"
                    )
            async for _ in app.push_many(orders):
                pass
            batch += 1
        await asyncio.sleep(min(2, max(0, deadline - loop.time())))


async def run(args):
    async with AsyncExitStack() as stack:
        apps = [
            await stack.enter_async_context(application(args.dsn))
            for _ in range(2)
        ]
        await apps[0].migrate()
        await prepare(apps[0])
        workers = [
            await stack.enter_async_context(
                Worker(
                    app,
                    tags={"reports"} if i == 0 else {"general"},
                    register_signal_handlers=False,
                )
            )
            for i, app in enumerate(apps)
        ]
        api = Api(authentication_backend=SimpleAuthBackend({"demo": "demo"}))
        server = uvicorn.Server(
            uvicorn.Config(
                api.build_starlette_app(workers[0], apps[0]),
                host="127.0.0.1",
                port=args.port,
                log_level="warning",
                timeout_graceful_shutdown=2,
            )
        )
        serving = asyncio.create_task(server.serve())
        print(
            f"Dashboard: http://127.0.0.1:{args.port} (demo / demo)\n"
            "Failures are intentional. Metrics need a few minutes to accumulate.\n"
            "Press Ctrl+C to stop; demo data is retained.",
            flush=True,
        )
        tasks = [
            asyncio.create_task(produce(apps[0], args.seed, args.duration))
        ]
        tasks.extend(
            asyncio.create_task(w.wait_for_shutdown()) for w in workers
        )
        try:
            done, _ = await asyncio.wait(
                [serving, *tasks], return_when=asyncio.FIRST_COMPLETED
            )
            for future in done:
                future.result()
        finally:
            for future in tasks:
                future.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            server.should_exit = True
            await asyncio.gather(serving, return_exceptions=True)


def main(coroutine):
    """Run either demo entry point with graceful shutdown and a Windows loop."""
    handlers = {}
    if sys.platform != "win32":
        for sig in (signal.SIGTERM, signal.SIGHUP):
            handlers[sig] = signal.signal(
                sig, lambda *_: signal.raise_signal(signal.SIGINT)
            )
    try:
        with asyncio.Runner(
            loop_factory=asyncio.SelectorEventLoop
            if sys.platform == "win32"
            else None
        ) as runner:
            runner.run(coroutine)
    except KeyboardInterrupt:
        pass
    finally:
        for sig, handler in handlers.items():
            signal.signal(sig, handler)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dsn",
        default=os.getenv(
            "CHANCY_DEMO_DSN",
            "postgresql://postgres:localtest@localhost:8190/postgres",
        ),
    )
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--duration",
        type=int,
        default=0,
        help="seconds to run before shutting down; 0 runs until stopped",
    )
    args = parser.parse_args()
    if args.duration < 0:
        parser.error("--duration must be nonnegative")
    main(run(args))
