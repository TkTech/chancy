import asyncio
import inspect
from contextlib import asynccontextmanager
from functools import partial
from unittest.mock import AsyncMock, Mock

import pytest
from psycopg import OperationalError, sql

from chancy import Job, Worker
from chancy.plugin import Plugin
from chancy.plugins.leadership import Leadership
from chancy.plugins.reprioritize import Reprioritize
from chancy.rule import JobRules
from chancy.session import (
    Authority,
    LeadershipLost,
    OperationContext,
    PluginSession,
)


class SessionPlugin(Plugin):
    @staticmethod
    def get_identifier():
        return "test.session"


async def context(worker, *, authority=Authority.WORKER):
    session = PluginSession(SessionPlugin(), worker)
    operation = session.periodic(
        "test", AsyncMock(), interval=1, authority=authority
    )
    return OperationContext(session, operation)


@pytest.mark.asyncio
async def test_setup_failure_is_fatal_and_releases_subscriptions(
    chancy_just_app,
):
    ran = AsyncMock()
    notified = AsyncMock()

    class Broken(SessionPlugin):
        async def setup(self, session):
            session.periodic("test", ran, interval=0)
            session.subscribe("test", notified)
            raise ValueError("setup failed")

    chancy_just_app.plugins = {"test.session": Broken()}
    chancy_just_app.is_up_to_date = AsyncMock(return_value=True)
    worker = Worker(chancy_just_app)
    with pytest.raises(ValueError, match="setup failed"):
        await worker.start()
    await worker.hub.emit("test", {})
    ran.assert_not_awaited()
    notified.assert_not_awaited()
    assert worker.shutdown_event.is_set()
    assert not worker.manager._tasks


@pytest.mark.asyncio
async def test_sessions_own_subscriptions_and_tasks(chancy_just_app):
    worker = Worker(chancy_just_app)
    session = PluginSession(SessionPlugin(), worker)
    entered = asyncio.Event()
    stopped = asyncio.Event()
    handler = AsyncMock()

    async def service(ctx):
        entered.set()
        try:
            await asyncio.Future()
        finally:
            stopped.set()

    session.subscribe("test", handler)
    session.service("service", service)
    session.start()
    await entered.wait()
    await worker.hub.emit("test", {})
    await session.close()
    await session.close()
    await worker.hub.emit("test", {})
    handler.assert_awaited_once()
    assert stopped.is_set()
    assert not worker.manager._tasks


@pytest.mark.asyncio
@pytest.mark.parametrize("event", ["test", None])
@pytest.mark.parametrize(
    "kind", ["sync", "async", "object", "wrapped", "future"]
)
async def test_subscription_awaits_handler_results(
    chancy_just_app, event, kind
):
    worker = Worker(chancy_just_app)
    session = PluginSession(SessionPlugin(), worker)
    received = []

    def synchronous(event):
        received.append(event)

    async def asynchronous(event):
        await asyncio.sleep(0)
        received.append(event)

    class Handler:
        async def __call__(self, event):
            await asynchronous(event)

    def future(event):
        result = asyncio.get_running_loop().create_future()

        def finish():
            received.append(event)
            result.set_result(None)

        asyncio.get_running_loop().call_soon(finish)
        return result

    handlers = {
        "sync": synchronous,
        "async": asynchronous,
        "object": Handler(),
        "wrapped": lambda event: asynchronous(event),
        "future": future,
    }
    session.subscribe(event, handlers[kind])
    await worker.hub.emit("test", {"value": 42})
    assert len(received) == 1
    assert received[0].name == "test"
    assert received[0].body == {"value": 42}
    await session.close()
    await worker.hub.emit("test", {})
    assert len(received) == 1


def test_registration_validation(chancy_just_app):
    session = PluginSession(SessionPlugin(), Worker(chancy_just_app))
    for interval in (-1, float("nan"), float("inf")):
        with pytest.raises(ValueError):
            session.periodic("invalid", AsyncMock(), interval=interval)
    with pytest.raises(TypeError):
        session.periodic("invalid", AsyncMock(), interval=1, authority="leader")
    session.periodic("one", AsyncMock(), interval=1)
    with pytest.raises(ValueError, match="Duplicate"):
        session.service("one", AsyncMock())


@pytest.mark.asyncio
async def test_failure_before_wait_is_still_observed(chancy_just_app):
    worker = Worker(chancy_just_app)
    session = PluginSession(SessionPlugin(), worker)
    operation = session.service(
        "broken", AsyncMock(side_effect=ValueError("broken"))
    )
    session.start()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    with pytest.raises(ValueError, match="broken"):
        await worker.wait_for_shutdown()
    assert operation.state == "failed"
    await session.close()


@pytest.mark.asyncio
async def test_periodic_wakeups_do_not_overlap_or_postpone_period(
    chancy_just_app,
):
    session = PluginSession(SessionPlugin(), Worker(chancy_just_app))
    active = 0
    calls = 0
    finished = asyncio.Event()

    async def tick(ctx):
        nonlocal active, calls
        active += 1
        assert active == 1
        await asyncio.sleep(0.005)
        calls += 1
        active -= 1
        if calls >= 3:
            finished.set()

    operation = session.periodic("tick", tick, interval=0.02)
    session.start()
    operation.wake_up()
    operation.wake_up()
    await asyncio.wait_for(finished.wait(), timeout=1)
    await session.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name",
    [
        "run",
        "on_worker_started",
        "sleep",
        "wait_for_leader",
        "wake_up",
        "cleanup_ex",
    ],
)
@pytest.mark.parametrize("inherited", [False, True])
async def test_removed_plugin_methods_fail_before_setup(
    chancy_just_app, name, inherited
):
    from chancy.plugins.recovery import Recovery

    old_method = AsyncMock()
    parent = type("OldPlugin", (Recovery,), {name: old_method})
    plugin_type = type("ChildPlugin", (parent,), {}) if inherited else parent
    plugin = plugin_type()
    plugin.setup = AsyncMock()
    chancy_just_app.plugins = {plugin.get_identifier(): plugin}
    chancy_just_app.is_up_to_date = AsyncMock(return_value=True)
    worker = Worker(chancy_just_app)
    with pytest.raises(TypeError, match=name):
        await worker.start()
    plugin.setup.assert_not_awaited()
    old_method.assert_not_awaited()
    chancy_just_app.is_up_to_date.assert_not_awaited()
    assert not worker.manager._tasks


@pytest.mark.asyncio
async def test_old_cleanup_signature_fails_before_setup(chancy_just_app):
    class OldCleanup(SessionPlugin):
        async def cleanup(self, chancy):
            raise AssertionError("must not run")

    plugin = OldCleanup()
    plugin.setup = AsyncMock()
    chancy_just_app.plugins = {plugin.get_identifier(): plugin}
    worker = Worker(chancy_just_app)
    with pytest.raises(TypeError, match=r"cleanup.*must accept"):
        await worker.start()
    plugin.setup.assert_not_awaited()


def test_async_cleanup_callback_is_rejected(chancy_just_app):
    session = PluginSession(SessionPlugin(), Worker(chancy_just_app))

    class AsyncCleanup:
        async def __call__(self):
            pass

    for callback in (AsyncMock(), AsyncCleanup(), partial(AsyncCleanup())):
        with pytest.raises(TypeError, match="must be synchronous"):
            session.callback(callback)


@pytest.mark.asyncio
async def test_cleanup_rejects_hidden_coroutine_and_releases_resources(
    chancy_just_app,
):
    session = PluginSession(SessionPlugin(), Worker(chancy_just_app))
    cleanup = Mock()

    async def asynchronous():
        pass

    coroutine = asynchronous()
    session.callback(cleanup, "resource", force=True)
    session.callback(lambda: coroutine)
    for _ in range(2):
        with pytest.raises(TypeError, match="must be synchronous"):
            await session.close()
    cleanup.assert_called_once_with("resource", force=True)
    assert inspect.getcoroutinestate(coroutine) == inspect.CORO_CLOSED


@pytest.mark.asyncio
async def test_cancelled_close_finishes_cleanup_for_other_callers(
    chancy_just_app,
):
    worker = Worker(chancy_just_app)
    session = PluginSession(SessionPlugin(), worker)
    running = asyncio.Event()
    stopping = asyncio.Event()
    release = asyncio.Event()
    cleanup = Mock()
    handler = AsyncMock()

    async def service(ctx):
        running.set()
        try:
            await asyncio.Future()
        finally:
            stopping.set()
            await release.wait()

    session.service("service", service)
    session.subscribe("test", handler)
    session.callback(cleanup)
    session.start()
    await asyncio.wait_for(running.wait(), timeout=1)
    first = asyncio.create_task(session.close())
    await asyncio.wait_for(stopping.wait(), timeout=1)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    cleanup.assert_not_called()
    second = asyncio.create_task(session.close())
    third = asyncio.create_task(session.close())
    release.set()
    await asyncio.wait_for(asyncio.gather(second, third), timeout=1)
    await session.close()
    await worker.hub.emit("test", {})
    cleanup.assert_called_once_with()
    handler.assert_not_awaited()
    assert not worker.manager._tasks


@pytest.mark.asyncio
async def test_worker_closes_all_sessions_after_cleanup_failure(
    chancy_just_app,
):
    worker = Worker(chancy_just_app)
    worker._drain_result = True
    worker.flush = AsyncMock()
    worker.is_leader.set()
    cleaned = []

    def cleanup(name, error):
        cleaned.append(name)
        if error:
            raise error

    for name, error in (
        ("first", None),
        ("second", ValueError("second failed")),
        ("third", RuntimeError("third failed")),
    ):
        session = PluginSession(SessionPlugin(), worker)
        session.callback(cleanup, name, error)
        worker._plugin_sessions.append(session)

    with pytest.raises(ExceptionGroup) as raised:
        await worker.stop()
    assert cleaned == ["third", "second", "first"]
    assert [str(exc) for exc in raised.value.exceptions] == [
        "third failed",
        "second failed",
    ]
    assert not worker.is_leader.is_set()


@pytest.mark.asyncio
async def test_database_retries_rollback_and_returns_only_committed_results(
    chancy,
):
    worker = Worker(chancy, backoff_initial=0)
    ctx = await context(worker)
    attempts = 0

    async def write(tx):
        nonlocal attempts
        attempts += 1
        await tx.cursor.execute(
            "CREATE TABLE IF NOT EXISTS session_rollback (id int)"
        )
        await tx.cursor.execute("INSERT INTO session_rollback VALUES (1)")
        if attempts == 1:
            raise OperationalError("disconnected")
        return 42

    try:
        assert await ctx.database(write) == 42
        async with chancy.pool.connection() as conn:
            assert (
                await (
                    await conn.execute("SELECT count(*) FROM session_rollback")
                ).fetchone()
            )[0] == 1
        assert attempts == 2
    finally:
        async with chancy.pool.connection() as conn:
            await conn.execute("DROP TABLE IF EXISTS session_rollback")


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [OperationalError, ValueError])
async def test_database_exhaustion_and_permanent_errors(chancy, error):
    worker = Worker(chancy, backoff_initial=0, backoff_max_retries=2)
    ctx = await context(worker)
    callback = AsyncMock(side_effect=error("failed"))
    with pytest.raises(error, match="failed"):
        await ctx.database(callback)
    assert callback.await_count == (3 if error is OperationalError else 1)


@pytest.mark.asyncio
async def test_cancellation_during_backoff(chancy):
    worker = Worker(chancy, backoff_initial=60)
    ctx = await context(worker)
    entered = asyncio.Event()

    async def fail(tx):
        entered.set()
        raise OperationalError("failed")

    task = asyncio.create_task(ctx.database(fail))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1)


@pytest.mark.asyncio
async def test_stale_leader_cannot_perform_database_effects(chancy):
    worker = Worker(chancy)
    ctx = await context(worker, authority=Authority.LEADER)
    worker.is_leader.set()
    async with chancy.pool.connection() as conn:
        await conn.execute(
            sql.SQL(
                "INSERT INTO {} (id, worker_id, expires_at) VALUES (1, 'other', now() + interval '1 hour')"
            ).format(sql.Identifier(f"{chancy.prefix}leader"))
        )
    effect = AsyncMock()
    with pytest.raises(LeadershipLost):
        await ctx.database(effect)
    effect.assert_not_awaited()
    assert not worker.is_leader.is_set()


@pytest.mark.asyncio
async def test_reprioritize_retries_uncertain_commits_without_double_increment(
    chancy, monkeypatch
):
    refs = [
        await chancy.push(Job(func="tests.test_plugin_sessions.test_job"))
        for _ in range(3)
    ]
    plugin = Reprioritize(
        JobRules.Age() >= 0, priority_increase=5, batch_size=2
    )
    worker = Worker(chancy, backoff_initial=0)
    worker.is_leader.set()
    worker._leadership_unfenced = True
    ctx = await context(worker, authority=Authority.LEADER)
    connection = chancy.pool.connection
    committed = False

    @asynccontextmanager
    async def lose_response(*args, **kwargs):
        nonlocal committed
        async with connection(*args, **kwargs) as conn:
            yield conn
        if not committed:
            async with connection() as conn:
                cursor = await conn.execute(
                    sql.SQL(
                        "SELECT count(*) FROM {} WHERE priority = 5"
                    ).format(sql.Identifier(f"{chancy.prefix}jobs"))
                )
                if (await cursor.fetchone())[0]:
                    committed = True
                    raise OperationalError("lost commit response")

    monkeypatch.setattr(chancy.pool, "connection", lose_response)
    await plugin.reprioritize_jobs(chancy, context=ctx)
    assert committed
    assert all(job.priority == 5 for job in await chancy.get_jobs(refs))


@pytest.mark.asyncio
async def test_leadership_lease_expires_without_renewal(chancy):
    plugin = Leadership(poll_interval=1, timeout=2)
    worker = Worker(chancy)
    session = PluginSession(plugin, worker)
    await plugin.setup(session)
    session.start()
    session.operations["renew"].wake_up()
    try:
        await asyncio.wait_for(worker.is_leader.wait(), timeout=1)
        await worker.manager.cancel("chancy.leadership:renew")
        async with asyncio.timeout(5):
            while worker.is_leader.is_set():
                await asyncio.sleep(0.01)
    finally:
        await session.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("outage", [0.1, 3], ids=["short", "past-lease"])
async def test_leadership_recovers_and_fences_old_worker(
    chancy, monkeypatch, outage
):
    from chancy import Chancy

    ref = await chancy.push(Job(func="tests.test_plugin_sessions.test_job"))
    first = Worker(
        chancy, worker_id="first", backoff_initial=0.01, backoff_max=0.01
    )
    leadership = Leadership(poll_interval=1, timeout=2)
    first_session = PluginSession(leadership, first)
    await leadership.setup(first_session)
    first_session.start()
    first_session.operations["renew"].wake_up()
    async with Chancy(chancy.dsn, prefix=chancy.prefix) as other:
        second = Worker(other, worker_id="second")
        second_session = PluginSession(leadership, second)
        try:
            await asyncio.wait_for(first.is_leader.wait(), timeout=2)
            await leadership.setup(second_session)
            second_session.start()
            connection = chancy.pool.connection
            failed = asyncio.Event()

            def disconnected(*args, **kwargs):
                failed.set()
                raise OperationalError("isolated worker")

            monkeypatch.setattr(chancy.pool, "connection", disconnected)
            first_session.operations["renew"].wake_up()
            await asyncio.wait_for(failed.wait(), timeout=2)
            await asyncio.sleep(outage)
            assert not first.is_leader.is_set()
            monkeypatch.setattr(chancy.pool, "connection", connection)
            if outage < leadership.timeout:
                await asyncio.wait_for(first.is_leader.wait(), timeout=2)
                assert not second.is_leader.is_set()
            else:
                await asyncio.wait_for(second.is_leader.wait(), timeout=5)
                first.is_leader.set()  # A stale local flag cannot authorize writes.
                old = await context(first, authority=Authority.LEADER)
                with pytest.raises(LeadershipLost):
                    await old.database(AsyncMock())
                current = await context(second, authority=Authority.LEADER)

                async def effect(tx):
                    await tx.cursor.execute(
                        sql.SQL(
                            "UPDATE {} SET priority = 7 WHERE id = %s"
                        ).format(sql.Identifier(f"{chancy.prefix}jobs")),
                        [ref.identifier],
                    )

                await current.database(effect)
                assert (await chancy.get_job(ref)).priority == 7
        finally:
            await first_session.close()
            await second_session.close()


@pytest.mark.asyncio
async def test_takeover_waits_for_protected_effect_transaction(chancy):
    first = Worker(chancy, worker_id="first")
    second = Worker(chancy, worker_id="second")
    ctx = await context(first, authority=Authority.LEADER)
    async with chancy.pool.connection() as conn:
        await conn.execute(
            sql.SQL(
                "INSERT INTO {} (id, worker_id, expires_at) "
                "VALUES (1, 'first', clock_timestamp() + interval '0.1 second')"
            ).format(sql.Identifier(f"{chancy.prefix}leader"))
        )
    entered, release = asyncio.Event(), asyncio.Event()

    async def effect(tx):
        entered.set()
        await release.wait()

    task = asyncio.create_task(ctx.database(effect))
    leadership = Leadership(poll_interval=1, timeout=2)
    session = PluginSession(leadership, second)
    try:
        await asyncio.wait_for(entered.wait(), timeout=1)
        await asyncio.sleep(0.12)
        await leadership.setup(session)
        session.start()
        session.operations["renew"].wake_up()
        await asyncio.sleep(0.04)
        assert not second.is_leader.is_set()
        release.set()
        await task
        await asyncio.wait_for(second.is_leader.wait(), timeout=2)
        with pytest.raises(LeadershipLost):
            await ctx.database(AsyncMock())
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await session.close()
