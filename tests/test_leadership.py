import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from psycopg import OperationalError, sql
from psycopg.rows import dict_row

from chancy import Worker
from chancy.errors import LeadershipLost
from chancy.plugin import Plugin
from chancy.session import Authority, PluginSession


class Observer(Plugin):
    @staticmethod
    def get_identifier():
        return "test.leadership"


@pytest.mark.parametrize(
    "interval,timeout",
    [
        (0, 1),
        (-1, 1),
        (2, 1),
        (1, 1),
        (1, 0),
        (1, -1),
        (float("nan"), 1),
        (1, float("nan")),
        (float("inf"), float("inf")),
        (1, float("inf")),
    ],
)
def test_invalid_lease_intervals(chancy_just_app, interval, timeout):
    with pytest.raises(ValueError, match="Leadership intervals"):
        Worker(
            chancy_just_app,
            leadership_poll_interval=interval,
            leadership_timeout=timeout,
        )


def test_leadership_is_read_only(chancy_just_app):
    worker = Worker(chancy_just_app)
    assert worker.is_leader is False
    with pytest.raises(AttributeError):
        worker.is_leader = True  # pyrefly: ignore[read-only]


@pytest.mark.parametrize("timeout", [0, -1, 1.5, float("nan"), float("inf")])
def test_invalid_leadership_idle_timeout(chancy_just_app, timeout):
    with pytest.raises(ValueError, match="leadership_idle_timeout"):
        Worker(chancy_just_app, leadership_idle_timeout=timeout)


@pytest.mark.asyncio
async def test_guard_idle_timeout_allows_long_sql(chancy):
    worker = Worker(chancy, leadership_idle_timeout=1)
    await worker._leadership.renew()
    async with (
        chancy.pool.connection() as conn,
        conn.transaction(),
        conn.cursor(row_factory=dict_row) as cursor,
    ):
        await worker._leadership.guard(cursor)
        await cursor.execute("SELECT pg_sleep(2), 42 AS value")
        assert (await cursor.fetchone())["value"] == 42


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chancy", [{"no_default_plugins": True}], indirect=True
)
async def test_election_is_core_and_stops_with_worker(chancy):
    assert not chancy.plugins
    async with Worker(chancy) as worker:
        await asyncio.wait_for(worker.wait_for_leadership(), 2)
        assert worker.is_leader is True
        assert {task.get_name() for task in worker.manager} >= {
            "leadership",
            "leadership_expiry",
        }
    assert worker.is_leader is False
    assert not list(worker.manager)


@pytest.mark.asyncio
async def test_ineligible_worker_cannot_run_leader_operations(chancy):
    async with Worker(chancy, leadership_eligible=False) as worker:
        assert not worker.is_leader
        assert not any(
            task.get_name().startswith("leadership") for task in worker.manager
        )
        session = PluginSession(Observer(), worker)
        callback = AsyncMock()
        session.periodic(
            "leader-only",
            callback,
            interval=1,
            immediate=True,
            authority=Authority.LEADER,
        )
        session.start()
        try:
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(worker.wait_for_leadership(), 0.05)
            callback.assert_not_awaited()
            # A matching lease cannot authorize an opted-out worker.
            async with (
                chancy.pool.connection() as conn,
                conn.cursor() as cursor,
            ):
                await cursor.execute(
                    sql.SQL(
                        "INSERT INTO {} (id, worker_id, expires_at) "
                        "VALUES (1, %s, clock_timestamp() + interval '1 hour')"
                    ).format(sql.Identifier(f"{chancy.prefix}leader")),
                    [worker.worker_id],
                )
                with pytest.raises(LeadershipLost):
                    await worker._leadership.guard(cursor)
        finally:
            await session.close()


@pytest.mark.asyncio
async def test_renewal_cannot_extend_another_workers_lease(chancy):
    worker = Worker(chancy)
    await worker._leadership.renew()
    assert worker.is_leader
    async with chancy.pool.connection() as conn:
        result = await conn.execute(
            sql.SQL(
                "UPDATE {} SET worker_id = 'other' RETURNING expires_at"
            ).format(sql.Identifier(f"{chancy.prefix}leader"))
        )
        expiry = (await result.fetchone())[0]
    await worker._leadership.renew()
    assert not worker.is_leader
    async with chancy.pool.connection() as conn:
        result = await conn.execute(
            sql.SQL("SELECT worker_id, expires_at FROM {}").format(
                sql.Identifier(f"{chancy.prefix}leader")
            )
        )
        assert await result.fetchone() == ("other", expiry)


@pytest.mark.asyncio
async def test_lease_expires_while_renewal_is_blocked(chancy, monkeypatch):
    worker = Worker(
        chancy, leadership_poll_interval=0.05, leadership_timeout=0.2
    )
    await worker._leadership.renew()
    assert worker.is_leader
    connection = chancy.pool.connection
    entered, release = asyncio.Event(), asyncio.Event()

    @asynccontextmanager
    async def delayed_connection(*args, **kwargs):
        entered.set()
        await release.wait()
        async with connection(*args, **kwargs) as conn:
            yield conn

    monkeypatch.setattr(chancy.pool, "connection", delayed_connection)
    renewal = asyncio.create_task(worker._leadership.renew())
    expiry = asyncio.create_task(worker._leadership.expire())
    try:
        await asyncio.wait_for(entered.wait(), 1)
        await asyncio.sleep(0.25)
        assert not worker.is_leader
        release.set()
        await asyncio.wait_for(renewal, 1)
        # A delayed response cannot revive local leadership beyond its deadline.
        assert not worker.is_leader
        await worker._leadership.renew()
        assert worker.is_leader
    finally:
        release.set()
        renewal.cancel()
        expiry.cancel()
        await asyncio.gather(renewal, expiry, return_exceptions=True)


@pytest.mark.asyncio
async def test_property_checks_expiry_without_expiry_task(chancy):
    worker = Worker(
        chancy, leadership_poll_interval=0.05, leadership_timeout=0.1
    )
    await worker._leadership.renew()
    assert worker.is_leader
    await asyncio.sleep(0.12)
    assert not worker.is_leader
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(worker.wait_for_leadership(), 0.02)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chancy", [{"no_default_plugins": True}], indirect=True
)
async def test_many_workers_elect_one_and_take_over_after_stop(chancy):
    async with AsyncExitStack() as stack:
        workers = [
            await stack.enter_async_context(
                Worker(
                    chancy,
                    leadership_poll_interval=0.05,
                    leadership_timeout=0.3,
                    register_signal_handlers=False,
                )
            )
            for _ in range(5)
        ]
        async with asyncio.timeout(3):
            while not any(worker.is_leader for worker in workers):
                await asyncio.sleep(0.01)
        leaders = [worker for worker in workers if worker.is_leader]
        assert len(leaders) == 1
        leader = leaders[0]
        await leader.stop()
        assert not leader.is_leader
        async with asyncio.timeout(3):
            while not any(worker.is_leader for worker in workers):
                await asyncio.sleep(0.01)
        assert sum(worker.is_leader for worker in workers) == 1
        assert not leader.is_leader


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [ValueError, OperationalError])
@pytest.mark.parametrize(
    "chancy", [{"no_default_plugins": True}], indirect=True
)
async def test_terminal_election_failure_stops_worker(
    chancy, monkeypatch, error
):
    worker = Worker(
        chancy,
        leadership_poll_interval=0.01,
        backoff_initial=0,
        backoff_max_retries=1,
    )
    await worker.start()
    await asyncio.wait_for(worker.wait_for_leadership(), 2)
    # Fail the database call, exercising the election's retry budget as well as supervision.
    monkeypatch.setattr(
        chancy, "notify", AsyncMock(side_effect=error("election failed"))
    )
    with pytest.raises(error, match="election failed"):
        await asyncio.wait_for(worker.wait_for_shutdown(), 3)
    assert not worker.healthy
    assert not worker.is_leader
    assert worker._finalized


@pytest.mark.asyncio
async def test_worker_listing_ignores_expired_lease(chancy):
    async with Worker(chancy, leadership_poll_interval=60) as worker:
        await asyncio.wait_for(worker.wait_for_leadership(), 2)
        async with chancy.pool.connection() as conn:
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET expires_at = clock_timestamp() - interval '1 second'"
                ).format(sql.Identifier(f"{chancy.prefix}leader"))
            )
        rows = await chancy.get_all_workers()
        assert rows
        assert not any(row["is_leader"] for row in rows)


@pytest.mark.asyncio
async def test_lease_notifications_are_delivered_to_other_workers(chancy):
    # Start an observer without allowing it to win the election.
    async with Worker(chancy, leadership_eligible=False) as observer:
        candidate = Worker(chancy)
        events = asyncio.Queue()
        observer.hub.on_any(lambda event: events.put_nowait(event))

        async def next_leadership_event():
            async with asyncio.timeout(2):
                while True:
                    event = await events.get()
                    if event.name.startswith("leadership."):
                        return event

        await candidate._leadership.renew()
        gained = await next_leadership_event()
        assert gained.name == "leadership.gained"
        assert gained.body["worker_id"] == candidate.worker_id
        await candidate._leadership.renew()
        assert (await next_leadership_event()).name == "leadership.renewed"
        async with chancy.pool.connection() as conn:
            await conn.execute(
                sql.SQL("UPDATE {} SET worker_id = 'replacement'").format(
                    sql.Identifier(f"{chancy.prefix}leader")
                )
            )
        await candidate._leadership.renew()
        assert (await next_leadership_event()).name == "leadership.lost"
        assert not candidate.is_leader


@pytest.mark.asyncio
async def test_shutdown_during_renewal_cannot_restore_local_leadership(
    chancy, monkeypatch
):
    worker = Worker(chancy)
    connection = chancy.pool.connection
    committed, release = asyncio.Event(), asyncio.Event()

    @asynccontextmanager
    async def delayed_commit_response(*args, **kwargs):
        async with connection(*args, **kwargs) as conn:
            yield conn
        committed.set()
        await release.wait()

    monkeypatch.setattr(chancy.pool, "connection", delayed_commit_response)
    renewal = asyncio.create_task(worker._leadership.renew())
    try:
        await asyncio.wait_for(committed.wait(), 1)
        worker.shutdown_event.set()
        release.set()
        await asyncio.wait_for(renewal, 1)
        assert not worker.is_leader
        assert not worker._leadership._elected.is_set()
    finally:
        release.set()
        renewal.cancel()
        await asyncio.gather(renewal, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "renewing_owner", [True, False], ids=["leader", "follower"]
)
async def test_guard_allows_lease_renewal(chancy, renewing_owner):
    leader = Worker(chancy)
    await leader._leadership.renew()
    contender = leader if renewing_owner else Worker(chancy)
    async with (
        chancy.pool.connection() as conn,
        conn.transaction(),
        conn.cursor(row_factory=dict_row) as cursor,
    ):
        await leader._leadership.guard(cursor)
        await asyncio.wait_for(contender._leadership.renew(), timeout=1)
        await cursor.execute(
            sql.SQL("SELECT worker_id FROM {} WHERE id = 1").format(
                sql.Identifier(f"{chancy.prefix}leader")
            )
        )
        assert (await cursor.fetchone())["worker_id"] == leader.worker_id
        assert contender.is_leader is renewing_owner


@pytest.mark.asyncio
async def test_guard_blocks_owner_change(chancy):
    leader = Worker(chancy)
    await leader._leadership.renew()
    entered = asyncio.Event()

    async def change_owner():
        async with chancy.pool.connection() as conn:
            entered.set()
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET worker_id = 'replacement' WHERE id = 1"
                ).format(sql.Identifier(f"{chancy.prefix}leader"))
            )

    task = None
    try:
        async with (
            chancy.pool.connection() as conn,
            conn.transaction(),
            conn.cursor(row_factory=dict_row) as cursor,
        ):
            await leader._leadership.guard(cursor)
            task = asyncio.create_task(change_owner())
            await asyncio.wait_for(entered.wait(), timeout=1)
            done, _ = await asyncio.wait({task}, timeout=0.05)
            assert not done
        await asyncio.wait_for(task, timeout=1)
        async with (
            chancy.pool.connection() as conn,
            conn.cursor(row_factory=dict_row) as cursor,
        ):
            with pytest.raises(LeadershipLost):
                await leader._leadership.guard(cursor)
    finally:
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_waiter_logs_expiry_once(chancy, monkeypatch, caplog):
    worker = Worker(chancy)
    await worker._leadership.renew()
    monkeypatch.setattr(worker._leadership, "_deadline", 0)
    waiter = asyncio.create_task(worker.wait_for_leadership())
    try:
        await asyncio.sleep(0)
        assert not worker.is_leader
        worker._leadership.clear()
        assert (
            sum(
                "lost leadership" in record.message for record in caplog.records
            )
            == 1
        )
    finally:
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)
