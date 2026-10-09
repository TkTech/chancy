import datetime
from contextlib import asynccontextmanager

import psycopg
import pytest
from psycopg import sql

from chancy import Worker
from chancy.plugins.metrics import Metrics
from chancy.plugins.metrics import metrics as module

pytestmark = pytest.mark.asyncio


@pytest.fixture
def clock(monkeypatch):
    now = [datetime.datetime(2026, 10, 8, 12, tzinfo=datetime.UTC)]
    monkeypatch.setattr(module, "utcnow", lambda: now[0])
    return now


async def test_aggregation_sessions_windows_and_gaps(chancy, clock):
    first, second = Metrics(), Metrics()
    first.worker_id = second.worker_id = "reused-worker-id"
    await first.increment_counter("queue:q:succeeded", 2)
    await first.record_histogram_value("queue:q:time", 2, unit="seconds")
    await first.record_gauge("table:jobs:size", 100, unit="bytes")
    await first.flush(chancy)
    await first.flush(chancy)
    clock[0] += datetime.timedelta(minutes=10)
    await second.increment_counter("queue:q:succeeded", 3)
    await second.record_histogram_value("queue:q:time", 4, unit="seconds")
    await second.record_histogram_value("queue:q:time", 12, unit="seconds")
    await second.record_gauge("table:jobs:size", 80, unit="bytes")
    await second.flush(chancy)
    result = await second.get_metrics(
        chancy, "queue:q", resolution="5min", range_seconds=900
    )
    assert result["end"] - result["start"] == datetime.timedelta(minutes=15)
    counter = result["series"]["queue:q:succeeded"]
    assert counter.summary == 5
    assert len(counter.data) == 2  # The middle bucket is unknown, not zero.
    assert counter.data[0].timestamp < counter.data[1].timestamp
    histogram = result["series"]["queue:q:time"]
    assert histogram.summary == {
        "count": 3,
        "sum": 18,
        "avg": 6,
        "min": 2,
        "max": 12,
    }
    assert histogram.unit == "seconds"
    gauge = (await second.get_metrics(chancy, "table:jobs:size"))["series"][
        "table:jobs:size"
    ]
    assert gauge.summary == 80
    assert gauge.sampled_at == clock[0]
    limited = await second.get_metrics(chancy, "queue:q:succeeded", limit=1)
    assert limited["series"]["queue:q:succeeded"].summary == 3


async def test_latest_gauge_and_weighted_histogram_within_bucket(chancy, clock):
    first, second = Metrics(), Metrics()
    first.worker_id, second.worker_id = "z", "a"
    await first.record_gauge("size", 100)
    await first.record_histogram_value("time", 2)
    await first.flush(chancy)
    clock[0] += datetime.timedelta(seconds=1)
    await second.record_gauge("size", 90)
    for _ in range(3):
        await second.record_histogram_value("time", 10)
    await second.flush(chancy)
    response = await first.get_metrics(chancy)
    assert response["series"]["size"].summary == 90
    assert response["series"]["time"].summary["avg"] == 8
    response = await first.get_metrics(chancy, worker_id="z")
    assert response["series"]["size"].summary == 100


async def test_ambiguous_commit_retries_without_double_count(
    chancy, monkeypatch
):
    metrics = Metrics()
    await metrics.increment_counter("committed", 7)
    connection = chancy.pool.connection

    @asynccontextmanager
    async def ambiguous_connection():
        async with connection() as conn:
            yield conn
        raise psycopg.OperationalError("lost commit acknowledgement")

    monkeypatch.setattr(chancy.pool, "connection", ambiguous_connection)
    with pytest.raises(psycopg.OperationalError):
        await metrics.flush(chancy)
    assert metrics._dirty
    monkeypatch.setattr(chancy.pool, "connection", connection)
    await metrics.flush(chancy)
    assert not metrics._dirty
    assert (await metrics.get_metrics(chancy))["series"][
        "committed"
    ].summary == 7


async def test_observations_during_flush_are_not_cleared(chancy, monkeypatch):
    metrics = Metrics()
    await metrics.increment_counter("concurrent", 1)
    connection = chancy.pool.connection

    @asynccontextmanager
    async def observing_connection():
        async with connection() as conn:
            yield conn
        await metrics.increment_counter("concurrent", 2)

    monkeypatch.setattr(chancy.pool, "connection", observing_connection)
    await metrics.flush(chancy)
    assert metrics._dirty
    monkeypatch.setattr(chancy.pool, "connection", connection)
    await metrics.flush(chancy)
    assert (await metrics.get_metrics(chancy))["series"][
        "concurrent"
    ].summary == 3


async def test_chunks_survive_repeated_flushes_and_expire(chancy, clock):
    metrics = Metrics()
    for _ in range(25):
        await metrics.increment_counter("ticks", 1)
        await metrics.flush(chancy)
        clock[0] += datetime.timedelta(minutes=1)
    series = (await metrics.get_metrics(chancy, "ticks", resolution="1min"))[
        "series"
    ]["ticks"]
    assert len(series.data) == 25
    assert series.summary == 25
    async with chancy.pool.connection() as conn:
        cursor = await conn.execute(
            sql.SQL("SELECT max(cardinality(buckets)) FROM {}").format(
                sql.Identifier(f"{chancy.prefix}metrics")
            )
        )
        assert (await cursor.fetchone())[0] <= 12
    clock[0] += datetime.timedelta(days=100)
    assert (await metrics.get_metrics(chancy, "ticks"))["series"][
        "ticks"
    ].summary is None
    assert await metrics.cleanup(chancy)
    assert await metrics.list_metrics(chancy) == []
    metrics._prune_buffer()
    assert not metrics._buckets


async def test_invalid_observations_do_not_poison_buffer():
    metrics = Metrics(max_buffered_points=4)
    for value in (float("nan"), float("inf"), True, "bad", 10**1000):
        await metrics.increment_counter("bad", value)
    for key in ("\x00", "\ud800"):
        await metrics.increment_counter(key, 1)
    await metrics.increment_counter("bad-unit", 1, unit="\x00")
    assert not metrics._dirty
    await metrics.increment_counter("valid", 1)
    await metrics.record_gauge("valid", 999)
    await metrics.increment_counter("overflow-buffer", 1)
    assert len(metrics._buckets) == 4
    assert all(point.total == 1 for point in metrics._buckets.values())


@pytest.mark.parametrize(
    "params",
    [
        {"resolution": "invalid"},
        {"limit": 0},
        {"limit": 100000},
        {"range_seconds": 1},
        {"range_seconds": 100000000},
        {"range_seconds": 3600, "limit": 5},
        {
            "start": datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC).replace(
                tzinfo=None
            )
        },
    ],
)
async def test_invalid_windows(params):
    with pytest.raises(ValueError):
        Metrics().window(**params)


async def test_graceful_shutdown_flushes_startup_observations(chancy):
    async with Worker(chancy, register_signal_handlers=False) as worker:
        await worker.increment_counter("startup", 4)
        await worker.record_gauge("temperature", 22, unit="celsius")
    metrics = chancy.plugins["chancy.metrics"]
    result = await metrics.get_metrics(chancy)
    assert result["series"]["startup"].summary == 4
    assert result["series"]["temperature"].unit == "celsius"


async def test_migration_flushes_history_and_preserves_jobs(chancy):
    from chancy import Job, Queue

    metrics = chancy.plugins["chancy.metrics"]
    await chancy.declare(Queue("preserved"))
    ref = await chancy.push(Job(func="example.task", queue="preserved"))
    await metrics.migrate(chancy, to_version=1)
    async with chancy.pool.connection() as conn:
        await conn.execute(
            sql.SQL("""INSERT INTO {} (metric_key,resolution,worker_id,timestamps,"values",metric_type)
            VALUES ('old','5min','worker',ARRAY[now()],ARRAY['1'::jsonb],'counter')""").format(
                sql.Identifier(f"{chancy.prefix}metrics")
            )
        )
    await metrics.migrate(chancy)
    assert await metrics.list_metrics(chancy) == []
    assert (await chancy.get_job(ref)).queue == "preserved"
    await metrics.increment_counter("new", 1)
    await metrics.flush(chancy)
    assert (await metrics.get_metrics(chancy))["series"]["new"].summary == 1


async def test_repeated_flush_does_not_rewrite_unchanged_chunks(chancy):
    metrics = Metrics()
    await metrics.increment_counter("written", 1)
    await metrics.flush(chancy)
    statement = sql.SQL(
        "SELECT xmin::text, ctid::text FROM {} ORDER BY resolution"
    ).format(sql.Identifier(f"{chancy.prefix}metrics"))
    async with chancy.pool.connection() as conn:
        before = await (await conn.execute(statement)).fetchall()
    await metrics.flush(chancy)
    async with chancy.pool.connection() as conn:
        after = await (await conn.execute(statement)).fetchall()
    assert before == after


async def test_keys_without_colons_long_names_and_literal_prefixes(chancy):
    metrics = Metrics()
    for key in (
        "simple",
        "workflow:" + "x" * 240 + ":execution_time",
        "queue:a_%:count",
        "queue:abc:count",
    ):
        await metrics.increment_counter(key, 1)
    await metrics.flush(chancy)
    assert "simple" in await metrics.list_metrics(chancy)
    response = await metrics.get_metrics(chancy, "queue:a_%")
    assert set(response["series"]) == {"queue:a_%:count"}


async def test_periodic_flush_failure_retries_without_stopping_worker(
    chancy, monkeypatch
):
    import asyncio

    metrics = chancy.plugins["chancy.metrics"]
    metrics.sync_interval = 0.01
    flush = metrics.flush
    recorded = asyncio.Event()
    succeeded = asyncio.Event()
    attempts = 0

    async def flaky_flush(app):
        nonlocal attempts
        # Startup flushes must not satisfy the test before "retry" is recorded.
        await recorded.wait()
        attempts += 1
        if attempts == 1:
            raise psycopg.OperationalError("temporary connection failure")
        await flush(app)
        succeeded.set()

    monkeypatch.setattr(metrics, "flush", flaky_flush)
    async with Worker(chancy, register_signal_handlers=False) as worker:
        try:
            await worker.increment_counter("retry", 5)
        finally:
            recorded.set()
        await asyncio.wait_for(succeeded.wait(), timeout=2)
        assert attempts >= 2
        assert (await metrics.get_metrics(chancy))["series"][
            "retry"
        ].summary == 5


async def test_reusing_plugin_with_a_new_worker_preserves_history(chancy):
    for _ in range(2):
        async with Worker(
            chancy, worker_id="stable-id", register_signal_handlers=False
        ) as worker:
            await worker.increment_counter("sessions", 2)
    metrics = chancy.plugins["chancy.metrics"]
    assert (await metrics.get_metrics(chancy))["series"][
        "sessions"
    ].summary == 4


async def test_custom_metrics_from_final_job_update_are_flushed(chancy):
    from dataclasses import replace

    from chancy import Job, Queue, QueuedJob
    from chancy.plugin import Plugin

    class CustomMetrics(Plugin):
        @staticmethod
        def get_identifier():
            return "test.final_metrics"

        async def on_job_updated(self, *, worker, job):
            await worker.increment_counter("custom:final_update", 1)

    chancy.plugins[CustomMetrics.get_identifier()] = CustomMetrics()
    await chancy.declare(Queue("paused", state=Queue.State.PAUSED))
    ref = await chancy.push(Job(func="example.task", queue="paused"))
    worker = Worker(chancy, register_signal_handlers=False)
    worker.send_outgoing_interval = 3600
    async with worker:
        queued = await chancy.get_job(ref)
        await worker.queue_update(
            replace(queued, state=QueuedJob.State.SUCCEEDED)
        )
    metrics = chancy.plugins["chancy.metrics"]
    response = await metrics.get_metrics(chancy, "custom")
    assert response["series"]["custom:final_update"].summary == 1
    assert worker not in metrics._workers
    await worker.increment_counter("custom:after_stop", 1)
    assert not metrics._dirty


async def test_shared_application_keeps_worker_sessions_separate(chancy):
    metrics = chancy.plugins["chancy.metrics"]
    async with Worker(
        chancy, worker_id="one", register_signal_handlers=False
    ) as first:
        await first.increment_counter("shared", 2)
        async with Worker(
            chancy, worker_id="two", register_signal_handlers=False
        ) as second:
            await second.increment_counter("shared", 3)
        await first.increment_counter("shared", 4)
        await metrics.flush(chancy)
        assert (await metrics.get_metrics(chancy, worker_id="one"))["series"][
            "shared"
        ].summary == 6
        assert (await metrics.get_metrics(chancy, worker_id="two"))["series"][
            "shared"
        ].summary == 3
    assert (await metrics.get_metrics(chancy))["series"]["shared"].summary == 9
    assert not metrics._workers
    assert not metrics._buckets


async def test_failed_shutdown_flush_keeps_observations_for_retry(
    chancy, monkeypatch
):
    metrics = chancy.plugins["chancy.metrics"]
    worker = Worker(chancy, register_signal_handlers=False)
    await worker.start()
    try:
        await worker.increment_counter("shutdown", 1)
        flush = metrics.flush

        async def fail(app):
            raise psycopg.OperationalError(
                "temporary shutdown persistence failure"
            )

        monkeypatch.setattr(metrics, "flush", fail)
        with pytest.raises(psycopg.OperationalError):
            await worker.stop()
        assert worker in metrics._workers
        await worker.increment_counter("shutdown", 2)
        monkeypatch.setattr(metrics, "flush", flush)
        await worker.stop()
        assert (await metrics.get_metrics(chancy))["series"][
            "shutdown"
        ].summary == 3
        assert not metrics._workers
    finally:
        monkeypatch.setattr(metrics, "flush", flush)
        await worker.stop()
