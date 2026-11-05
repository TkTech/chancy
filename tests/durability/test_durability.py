from __future__ import annotations

import asyncio
from datetime import datetime
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from psycopg import AsyncConnection, OperationalError, sql
from psycopg.rows import dict_row

from chancy import Chancy, Queue, QueuedJob, Reference, Worker
from chancy.errors import LeadershipLost
from chancy.hub import Event
from chancy.plugins.workflow import Workflow, WorkflowPlugin
from chancy.retry import DatabaseRetry
from tests.durability.jobs import (
    FirstExecutionGate,
    hold_first_execution,
    quick_job,
)
from tests.helpers import wait_until

if TYPE_CHECKING:
    from tests.durability.conftest import Cluster

pytestmark = [pytest.mark.asyncio, pytest.mark.timeout(60)]


async def succeeded(app: Chancy, ref: Reference) -> QueuedJob:
    result = await app.wait_for_job(ref, interval=1, timeout=15)
    assert result is not None
    assert result.state == QueuedJob.State.SUCCEEDED
    return result


async def heartbeat(
    cluster: Cluster, worker: Worker, *, after: datetime | None = None
) -> datetime:
    async with asyncio.timeout(15):
        while True:
            for row in await cluster.rows("workers"):
                if row["worker_id"] == worker.worker_id and (
                    after is None or row["last_seen"] > after
                ):
                    return row["last_seen"]
            await asyncio.sleep(0.05)


async def notification(cluster: Cluster, worker: Worker) -> None:
    received = asyncio.Event()
    token = uuid4().hex

    def on_probe(event: Event) -> None:
        if event.body["token"] == token:
            received.set()

    worker.hub.on("durability.probe", on_probe)
    try:
        async with asyncio.timeout(15):
            while not received.is_set():
                async with (
                    cluster.app.pool.connection() as conn,
                    conn.cursor(row_factory=dict_row) as cursor,
                ):
                    await cluster.app.notify(
                        cursor, "durability.probe", {"token": token}
                    )
                await asyncio.sleep(0.1)
    finally:
        worker.hub.remove("durability.probe", on_probe)


async def guarded(worker: Worker) -> None:
    async with (
        await AsyncConnection.connect(worker.chancy.dsn) as conn,
        conn.transaction(),
        conn.cursor(row_factory=dict_row) as cursor,
    ):
        await worker._leadership.guard(cursor)


async def pair(
    cluster: Cluster, *, leadership_timeout: int = 3
) -> tuple[Worker, Worker]:
    leader = await cluster.start_worker(
        "a", leadership_timeout=leadership_timeout
    )
    await asyncio.wait_for(leader.wait_for_leadership(), timeout=15)
    follower = await cluster.start_worker(
        "b", leadership_timeout=leadership_timeout
    )
    assert not follower.is_leader
    return leader, follower


async def test_reconnect_resumes_jobs_updates_and_notifications(
    cluster: Cluster,
    first_execution: FirstExecutionGate,
    monkeypatch: pytest.MonkeyPatch,
):
    worker = await cluster.start_worker()
    ref = await cluster.app.push(hold_first_execution)
    await asyncio.wait_for(first_execution.started.wait(), timeout=15)
    previous = await heartbeat(cluster, worker)

    queue_retried = asyncio.Event()
    queue_task = worker._queue_tasks["default"]
    wait = DatabaseRetry.wait

    async def observe_retry(retry: DatabaseRetry, error: BaseException) -> None:
        await wait(retry, error)
        if asyncio.current_task() is queue_task:
            queue_retried.set()

    monkeypatch.setattr(DatabaseRetry, "wait", observe_retry)

    async with cluster.proxies["a"].disconnected():
        executor = worker.executors["default"]
        worker.queue_wake_events["default"].set()
        await asyncio.wait_for(queue_retried.wait(), timeout=15)
        assert worker.executors["default"] is executor
        assert executor.is_job_running(ref)
        first_execution.release.set()
        await wait_until(
            lambda: bool(worker._pending_updates) or not worker.outgoing.empty()
        )
        running = await cluster.app.get_job(ref)
        assert running is not None
        assert running.state == QueuedJob.State.RUNNING
        missed = await cluster.app.push(quick_job)

    await succeeded(cluster.app, ref)
    await succeeded(cluster.app, missed)
    await heartbeat(cluster, worker, after=previous)
    await notification(cluster, worker)
    await asyncio.wait_for(worker.wait_for_leadership(), timeout=15)
    assert worker.healthy

    previous = await heartbeat(cluster, worker)
    async with cluster.proxies["a"].disconnected():
        missed = await cluster.app.push(quick_job)
        await wait_until(lambda: not worker.is_leader)
    await succeeded(cluster.app, missed)
    await heartbeat(cluster, worker, after=previous)
    await notification(cluster, worker)
    assert worker.healthy


async def test_stalled_connection_expires_local_leadership(cluster: Cluster):
    worker = await cluster.start_worker()
    await asyncio.wait_for(worker.wait_for_leadership(), timeout=15)
    proxy = cluster.proxies["a"]
    async with proxy.toxic("timeout", timeout=0):
        await wait_until(lambda: not worker.is_leader)
        assert worker.healthy
        assert not worker.manager.errors
    await asyncio.wait_for(worker.wait_for_leadership(), timeout=15)
    await succeeded(cluster.app, await cluster.app.push(quick_job))
    await notification(cluster, worker)
    assert worker.healthy


async def test_silent_guarded_transaction_releases_takeover_lock(
    cluster: Cluster,
):
    leader, follower = await pair(cluster)
    await cluster.app.declare(Queue("protected", tags=set()))
    ref = await cluster.app.push(quick_job.job.with_queue("protected"))
    conn = await AsyncConnection.connect(leader.chancy.dsn)
    try:
        async with conn.cursor(row_factory=dict_row) as cursor:
            await leader._leadership.guard(cursor)
            await cursor.execute(
                sql.SQL("UPDATE {} SET priority = 99 WHERE id = %s").format(
                    sql.Identifier(f"{cluster.app.prefix}jobs")
                ),
                [ref.identifier],
            )
            await cursor.execute("SELECT pg_backend_pid() AS pid")
            backend = await cursor.fetchone()
            assert backend is not None

            async with (
                cluster.proxies["a"].toxic(
                    "timeout", stream="upstream", timeout=0
                ),
                cluster.proxies["a"].toxic(
                    "timeout", stream="downstream", timeout=0
                ),
            ):
                await asyncio.wait_for(
                    follower.wait_for_leadership(), timeout=6
                )
                assert not leader.is_leader
                async with cluster.app.pool.connection() as observer:
                    result = await observer.execute(
                        "SELECT pid FROM pg_stat_activity WHERE pid = %s",
                        [backend["pid"]],
                    )
                    assert await result.fetchone() is None
                job = await cluster.app.get_job(ref)
                assert job is not None
                assert job.priority == 0
                await guarded(follower)
    finally:
        await conn.close()


async def test_retry_exhaustion_stops_without_a_waiter(cluster: Cluster):
    worker = await cluster.start_worker(backoff_max_retries=1)
    async with cluster.proxies["a"].disconnected():
        await asyncio.wait_for(worker.shutdown_event.wait(), timeout=15)
        assert not worker.healthy
        assert worker.manager.errors
        assert all(
            isinstance(e, OperationalError) for e in worker.manager.errors
        )

    with pytest.raises((OperationalError, ExceptionGroup)):
        await worker.wait_for_shutdown()
    assert worker._finalized
    assert not list(worker.manager)
    with pytest.raises((OperationalError, ExceptionGroup)):
        await worker.wait_for_shutdown()


async def test_shutdown_preserves_outstanding_completion(
    cluster: Cluster, first_execution: FirstExecutionGate
):
    worker = await cluster.start_worker()
    ref = await cluster.app.push(hold_first_execution)
    await asyncio.wait_for(first_execution.started.wait(), timeout=15)

    async with cluster.proxies["a"].disconnected():
        first_execution.release.set()
        await wait_until(
            lambda: bool(worker._pending_updates) or not worker.outgoing.empty()
        )
        stopping = asyncio.create_task(worker.stop())
        await asyncio.wait_for(worker.shutdown_event.wait(), timeout=15)
        running = await cluster.app.get_job(ref)
        assert running is not None
        assert running.state == QueuedJob.State.RUNNING

    try:
        await stopping
    except OperationalError:
        async with asyncio.timeout(15):
            while not worker._finalized:
                try:
                    await worker.stop()
                except OperationalError:
                    await asyncio.sleep(0.1)
    await succeeded(cluster.app, ref)
    assert worker._finalized
    assert not list(worker.manager)


@pytest.mark.parametrize("isolated", ["a", "b"], ids=["leader", "follower"])
async def test_short_partition_preserves_owner(cluster: Cluster, isolated: str):
    leader, follower = await pair(cluster, leadership_timeout=30)
    healthy = follower if isolated == "a" else leader
    previous = await heartbeat(cluster, healthy)
    async with cluster.proxies[isolated].disconnected():
        await succeeded(cluster.app, await cluster.app.push(quick_job))
        assert (await cluster.rows("jobs"))[0]["taken_by"] == healthy.worker_id
        await heartbeat(cluster, healthy, after=previous)
        lease = (await cluster.rows("leader"))[0]
        assert lease["worker_id"] == leader.worker_id

    await asyncio.wait_for(leader.wait_for_leadership(), timeout=15)
    await guarded(leader)
    for worker in (leader, follower):
        previous = await heartbeat(cluster, worker)
        await heartbeat(cluster, worker, after=previous)
        await notification(cluster, worker)
        assert worker.healthy
    assert not follower.is_leader


async def test_leader_partition_transfers_workflow_authority(cluster: Cluster):
    leader, follower = await pair(cluster)
    async with cluster.proxies["a"].disconnected():
        await asyncio.wait_for(follower.wait_for_leadership(), timeout=15)
        assert not leader.is_leader
        workflow_id = await WorkflowPlugin.push(
            cluster.app,
            Workflow("failover")
            .add("first", quick_job)
            .add("second", quick_job, ["first"]),
        )
        result = await WorkflowPlugin.wait_for_workflow(
            cluster.app, workflow_id, timeout=15
        )
        assert result.state == Workflow.State.COMPLETED

    await notification(cluster, leader)
    previous = await heartbeat(cluster, leader)
    await heartbeat(cluster, leader, after=previous)
    with pytest.raises(LeadershipLost):
        await guarded(leader)
    await guarded(follower)
    assert (await cluster.rows("leader"))[0]["worker_id"] == "b"
    assert leader.healthy and follower.healthy


async def test_recovery_fences_late_completion(
    cluster: Cluster, first_execution: FirstExecutionGate
):
    original = await cluster.start_worker("a", heartbeat_timeout=3)
    await asyncio.wait_for(original.wait_for_leadership(), timeout=15)
    ref = await cluster.app.push(
        hold_first_execution.job.with_kwargs(fail_first=True)
    )
    await asyncio.wait_for(first_execution.started.wait(), timeout=15)
    old = await cluster.app.get_job(ref)
    assert old is not None
    replacement = await cluster.start_worker("b", heartbeat_timeout=3)

    async with cluster.proxies["a"].disconnected():
        result = await succeeded(cluster.app, ref)
        assert (await cluster.rows("jobs"))[0][
            "taken_by"
        ] == replacement.worker_id
        assert result.claim_id != old.claim_id
        assert first_execution.calls == 2
        first_execution.release.set()
        await wait_until(
            lambda: (
                bool(original._pending_updates) or not original.outgoing.empty()
            )
        )

    await wait_until(
        lambda: not original._pending_updates and original.outgoing.empty()
    )
    await original.flush()
    assert await cluster.app.get_job(ref) == result
    assert original.healthy and replacement.healthy


async def test_cluster_outage_recovers_processing_and_one_leader(
    cluster: Cluster,
):
    first, second = await pair(cluster)
    async with (
        cluster.proxies["a"].disconnected(),
        cluster.proxies["b"].disconnected(),
    ):
        workflow_id = await WorkflowPlugin.push(
            cluster.app, Workflow("missed_notification").add("step", quick_job)
        )
        async with asyncio.timeout(15):
            while True:
                async with cluster.app.pool.connection() as conn:
                    clock = await conn.execute("SELECT clock_timestamp()")
                    now = await clock.fetchone()
                    assert now is not None
                if (await cluster.rows("leader"))[0]["expires_at"] <= now[0]:
                    break
                await asyncio.sleep(0.05)
        assert not first.is_leader and not second.is_leader

    result = await WorkflowPlugin.wait_for_workflow(
        cluster.app, workflow_id, timeout=15
    )
    assert result.state == Workflow.State.COMPLETED
    await wait_until(lambda: sum(w.is_leader for w in (first, second)) == 1)
    leases = await cluster.rows("leader")
    assert len(leases) == 1
    for worker in (first, second):
        previous = await heartbeat(cluster, worker)
        await heartbeat(cluster, worker, after=previous)
        await notification(cluster, worker)
        assert worker.healthy
        if worker.worker_id == leases[0]["worker_id"]:
            await guarded(worker)
        else:
            with pytest.raises(LeadershipLost):
                await guarded(worker)
