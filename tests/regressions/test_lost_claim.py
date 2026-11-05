import asyncio
from collections import Counter
from dataclasses import replace
from uuid import UUID

import pytest
from psycopg import AsyncCursor, AsyncTransaction, OperationalError, sql
from psycopg.errors import LockNotAvailable

from chancy import Chancy, Queue, QueuedJob, Worker, job
from chancy.errors import UnconfirmedClaims
from chancy.executors.asyncex import AsyncExecutor
from chancy.plugins.recovery import Recovery
from tests.helpers import wait_until

completed_jobs: list[UUID] = []


@job()
async def complete(*, context: QueuedJob):
    completed_jobs.append(context.id)


@pytest.fixture
def executions(monkeypatch):
    recorded = []
    monkeypatch.setitem(globals(), "completed_jobs", recorded)
    return recorded


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "commit", [True, False], ids=["committed", "rolled_back"]
)
@pytest.mark.parametrize("confirmation_error", [False, True])
@pytest.mark.parametrize(
    "chancy",
    [{"plugins": [Recovery(poll_interval=1)], "no_default_plugins": True}],
    indirect=True,
)
async def test_claim_survives_lost_response(
    chancy, monkeypatch, executions, commit, confirmation_error
):
    await chancy.declare(
        Queue(
            "default",
            executor=Chancy.Executor.Async,
            concurrency=1,
            polling_interval=1,
        )
    )
    ref = await chancy.push(complete)
    worker = Worker(
        chancy,
        register_signal_handlers=False,
        heartbeat_poll_interval=1,
        leadership_poll_interval=1,
        leadership_timeout=3,
        backoff_initial=0,
    )
    lost = asyncio.Event()
    exit_transaction = AsyncTransaction.__aexit__
    claim_id = None

    async def lose_commit_response(transaction, exc_type, exc_val, exc_tb):
        nonlocal claim_id
        if (
            exc_type is None
            and not transaction.savepoint_name
            and not lost.is_set()
            and asyncio.current_task() is worker._queue_tasks.get("default")
        ):
            result = await transaction.connection.execute(
                sql.SQL("SELECT claim_id FROM {} WHERE id = %s").format(
                    sql.Identifier(f"{chancy.prefix}jobs")
                ),
                [ref.identifier],
            )
            claim_id = (await result.fetchone())[0]
            assert claim_id is not None
            if commit:
                await exit_transaction(transaction, exc_type, exc_val, exc_tb)
            await transaction.connection.close()
            # Check the actual outcome from a separate connection. The queue
            # receives the same error whether the transaction committed or not.
            claimed = await chancy.get_job(ref)
            assert claimed.state == (
                QueuedJob.State.RUNNING if commit else QueuedJob.State.PENDING
            )
            lost.set()
            raise OperationalError("Connection lost during claim commit")
        return await exit_transaction(transaction, exc_type, exc_val, exc_tb)

    resolve = worker._resolve_claims
    resolutions = 0

    async def resolve_claims(jobs):
        nonlocal resolutions
        resolutions += 1
        result = await resolve(jobs)
        if confirmation_error and resolutions == 1:
            raise OperationalError("Connection lost during claim confirmation")
        return result

    monkeypatch.setattr(AsyncTransaction, "__aexit__", lose_commit_response)
    monkeypatch.setattr(worker, "_resolve_claims", resolve_claims)
    async with worker:
        await asyncio.wait_for(lost.wait(), timeout=5)
        following = await chancy.push(complete)
        completed = await chancy.wait_for_job(following, interval=1, timeout=10)
        assert completed.state == QueuedJob.State.SUCCEEDED
        assert worker.healthy
        completed = await chancy.wait_for_job(ref, interval=1, timeout=10)
        assert completed.state == QueuedJob.State.SUCCEEDED
        assert (completed.claim_id == claim_id) is commit
        assert Counter(executions) == {
            ref.identifier: 1,
            following.identifier: 1,
        }
        assert resolutions == (2 if confirmation_error else 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("commit", [True, False])
async def test_resolution_retries_until_original_transaction_ends(
    chancy, worker_no_start, commit
):
    queue = await chancy.declare(Queue("default"))
    await chancy.push(complete)
    async with chancy.pool.connection() as conn:
        async with conn.transaction(force_rollback=not commit):
            jobs = await worker_no_start.fetch_jobs(queue, conn)
            resolved = await asyncio.wait_for(
                worker_no_start._resolve_claims(jobs), timeout=2
            )
            assert resolved is None

        resolved = await worker_no_start._resolve_claims(jobs)
        assert resolved == (jobs if commit else [])


@pytest.mark.asyncio
@pytest.mark.parametrize("chancy", [{"notifications": False}], indirect=True)
async def test_confirmation_contention_waits_without_exhausting_retries(
    chancy, monkeypatch, executions
):
    await chancy.declare(
        Queue("default", executor=Chancy.Executor.Async, polling_interval=1)
    )
    worker = Worker(chancy, backoff_initial=0, backoff_max_retries=1)
    fetch = worker.fetch_jobs
    resolve = worker._resolve_claims
    lost = False
    attempts = []

    async def resolve_claims(jobs):
        attempts.append(asyncio.get_running_loop().time())
        return await resolve(jobs)

    monkeypatch.setattr(worker, "_resolve_claims", resolve_claims)
    async with worker:
        async with chancy.pool.connection() as holder, holder.transaction():

            async def unconfirmed_fetch(queue, conn, **kwargs):
                nonlocal lost
                jobs = await fetch(queue, conn, **kwargs)
                if jobs and not lost:
                    lost = True
                    await holder.execute(
                        sql.SQL(
                            "SELECT id FROM {} WHERE id = ANY(%s) FOR UPDATE"
                        ).format(sql.Identifier(f"{chancy.prefix}jobs")),
                        [[job.id for job in jobs]],
                    )
                    raise UnconfirmedClaims(jobs)
                return jobs

            monkeypatch.setattr(worker, "fetch_jobs", unconfirmed_fetch)
            ref = await chancy.push(complete)
            worker.queue_wake_events["default"].set()
            await wait_until(lambda: len(attempts) >= 3, timeout=5)
            assert worker.healthy
            assert executions == []
            assert len(attempts) <= 4
            assert attempts[2] - attempts[0] >= 1.5

        completed = await chancy.wait_for_job(ref, timeout=10)
        assert completed.state == QueuedJob.State.SUCCEEDED
        assert executions == [ref.identifier]
        assert worker.healthy


@pytest.mark.asyncio
async def test_confirmation_preserves_masked_cancellation(
    chancy, worker_no_start, monkeypatch
):
    queue = await chancy.declare(Queue("default"))
    await chancy.push(complete)
    execute = AsyncCursor.execute

    async def mask_cancellation(cursor, *args, **kwargs):
        try:
            return await execute(cursor, *args, **kwargs)
        except LockNotAvailable as exc:
            task = asyncio.current_task()
            assert task is not None
            task.cancel()
            try:
                await asyncio.sleep(0)
            except asyncio.CancelledError:
                # Psycopg can replace cancellation with the query's error.
                raise exc

    monkeypatch.setattr(AsyncCursor, "execute", mask_cancellation)
    async with chancy.pool.connection() as conn, conn.transaction():
        jobs = await worker_no_start.fetch_jobs(queue, conn)
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(
                worker_no_start._resolve_claims(jobs), timeout=2
            )


@pytest.mark.asyncio
async def test_confirmation_propagates_contention_after_failed_rollback(
    chancy, worker_no_start, monkeypatch
):
    queue = await chancy.declare(Queue("default"))
    await chancy.push(complete)
    exit_transaction = AsyncTransaction.__aexit__
    rollback_failed = False

    async def fail_rollback(transaction, exc_type, exc_val, exc_tb):
        nonlocal rollback_failed
        if isinstance(exc_val, LockNotAvailable):
            rollback_failed = True
            await transaction.connection.close()
        return await exit_transaction(transaction, exc_type, exc_val, exc_tb)

    monkeypatch.setattr(AsyncTransaction, "__aexit__", fail_rollback)
    async with chancy.pool.connection() as conn, conn.transaction():
        jobs = await worker_no_start.fetch_jobs(queue, conn)
        with pytest.raises(LockNotAvailable):
            await asyncio.wait_for(
                worker_no_start._resolve_claims(jobs), timeout=2
            )
    assert rollback_failed


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["cancel", "reclaim", "delete"])
async def test_resolution_preserves_only_current_claims(
    chancy, worker_no_start, change
):
    queue = await chancy.declare(Queue("default"))
    refs = [await chancy.push(complete) for _ in range(3)]
    async with chancy.pool.connection() as conn:
        jobs = await worker_no_start.fetch_jobs(queue, conn, up_to=3)

    if change == "cancel":
        await chancy.cancel_job(refs[1])
    elif change == "delete":
        await chancy.purge_jobs([refs[1]])
    else:
        async with chancy.pool.connection() as conn:
            await conn.execute(
                sql.SQL(
                    "UPDATE {} SET claim_id = gen_random_uuid() WHERE id = %s"
                ).format(sql.Identifier(f"{chancy.prefix}jobs")),
                [refs[1].identifier],
            )

    assert await worker_no_start._resolve_claims(jobs) == [
        job for job in jobs if job.id != refs[1].identifier
    ]


@pytest.mark.asyncio
async def test_partial_dispatch_retains_remaining_claims(
    chancy, monkeypatch, executions
):
    await chancy.declare(
        Queue("default", executor=Chancy.Executor.Async, concurrency=3)
    )
    refs = [await chancy.push(complete) for _ in range(3)]
    push = AsyncExecutor.push
    calls = 0

    async def fail_second_push(executor, job):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OperationalError("Connection lost in on_job_starting")
        return await push(executor, job)

    monkeypatch.setattr(AsyncExecutor, "push", fail_second_push)
    async with Worker(chancy, backoff_initial=0) as worker:
        completed = await chancy.wait_for_jobs(refs, timeout=10)
        assert all(job.state == QueuedJob.State.SUCCEEDED for job in completed)
        assert Counter(executions) == {ref.identifier: 1 for ref in refs}
        assert calls == 4
        assert worker.healthy


@pytest.mark.asyncio
async def test_nested_fetch_leaves_commit_errors_to_caller(
    chancy, worker_no_start, monkeypatch
):
    queue = await chancy.declare(Queue("default"))
    ref = await chancy.push(complete)
    exit_transaction = AsyncTransaction.__aexit__

    async def fail_savepoint(transaction, exc_type, exc_val, exc_tb):
        result = await exit_transaction(transaction, exc_type, exc_val, exc_tb)
        if transaction.savepoint_name and exc_type is None:
            raise OperationalError("Savepoint release failed")
        return result

    monkeypatch.setattr(AsyncTransaction, "__aexit__", fail_savepoint)
    async with chancy.pool.connection() as conn:
        with pytest.raises(OperationalError) as error:
            async with conn.transaction():
                await worker_no_start.fetch_jobs(queue, conn)
        assert not isinstance(error.value, UnconfirmedClaims)
    assert (await chancy.get_job(ref)).state == QueuedJob.State.PENDING


@pytest.mark.asyncio
@pytest.mark.parametrize("removed", [False, True])
async def test_queue_change_preserves_unconfirmed_claims(
    chancy, monkeypatch, executions, removed
):
    await chancy.declare(
        Queue("default", executor=Chancy.Executor.Async, polling_interval=1)
    )
    ref = await chancy.push(complete)
    worker = Worker(chancy, queue_change_poll_interval=1, backoff_initial=0)
    fetch = worker.fetch_jobs
    changed = False

    async def fetch_then_change_queue(queue, conn, **kwargs):
        nonlocal changed
        jobs = await fetch(queue, conn, **kwargs)
        if jobs and not changed:
            changed = True
            replacement = (
                replace(queue, tags={"unmatched"})
                if removed
                else replace(queue, concurrency=2)
            )
            await chancy.declare(replacement, upsert=True)
            await wait_until(lambda: worker._queues.get(queue.name) != queue)
            raise UnconfirmedClaims(jobs)
        return jobs

    monkeypatch.setattr(worker, "fetch_jobs", fetch_then_change_queue)
    async with worker:
        completed = await chancy.wait_for_job(ref, timeout=10)
        assert completed.state == QueuedJob.State.SUCCEEDED
        assert executions == [ref.identifier]
        assert worker.healthy


@pytest.mark.asyncio
async def test_confirmation_retry_exhaustion_stops_worker(chancy, monkeypatch):
    await chancy.declare(Queue("default", executor=Chancy.Executor.Async))
    worker = Worker(chancy, backoff_initial=0, backoff_max_retries=1)
    fetch = worker.fetch_jobs

    async def unconfirmed_fetch(queue, conn, **kwargs):
        jobs = await fetch(queue, conn, **kwargs)
        if jobs:
            raise UnconfirmedClaims(jobs)
        return jobs

    async def fail_confirmation(jobs):
        raise OperationalError("Confirmation unavailable")

    monkeypatch.setattr(worker, "fetch_jobs", unconfirmed_fetch)
    monkeypatch.setattr(worker, "_resolve_claims", fail_confirmation)
    with pytest.raises(OperationalError, match="Confirmation unavailable"):
        async with worker:
            await chancy.push(complete)
            await asyncio.wait_for(worker.wait_for_shutdown(), timeout=5)
    assert worker.shutdown_event.is_set()
    assert worker._finalized
