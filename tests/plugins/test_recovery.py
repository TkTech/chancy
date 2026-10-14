import asyncio
import json
import os
import signal
from concurrent.futures import Future
from dataclasses import replace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from psycopg import AsyncConnection, sql
from psycopg.rows import dict_row

from chancy import Job, Queue, QueuedJob, Reference, Worker
from chancy.executors.asyncex import AsyncExecutor
from chancy.executors.process import ProcessExecutor
from chancy.executors.thread import ThreadedExecutor
from chancy.hub import Event
from chancy.migrate import Migrator
from chancy.plugins.recovery import Recovery
from chancy.plugins.workflow import WorkflowPlugin


async def wait_until_cancelled():
    await asyncio.Future()


def receive_process_signal(*, requests=None, context: QueuedJob):
    if requests is not None:
        requests[context.claim_id] = True
    os.kill(os.getpid(), signal.SIGUSR1)
    return os.getpid()


async def claim(chancy, worker, queue):
    async with chancy.pool.connection() as conn:
        return (await worker.fetch_jobs(queue, conn))[0]


async def recover(chancy, worker):
    async with (
        chancy.pool.connection() as conn,
        conn.cursor(row_factory=dict_row) as cursor,
    ):
        return await Recovery.recover(worker, chancy, cursor)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reclaim, notifications", [(False, True), (True, True), (True, False)]
)
@pytest.mark.parametrize(
    "state", [QueuedJob.State.SUCCEEDED, QueuedJob.State.FAILED]
)
async def test_recovery_fences_late_updates_and_hooks(
    chancy, worker_no_start, monkeypatch, reclaim, notifications, state
):
    chancy.notifications = notifications
    queue = await chancy.declare(Queue("default"))
    ref = await chancy.push(Job(func="unused", unique_key="recovered"))
    old = await claim(chancy, worker_no_start, queue)
    assert old.claim_id is not None
    assert await recover(chancy, worker_no_start) == 1

    # Reuse the worker ID too: ownership must identify the execution, not
    # merely its worker. The stale update is unsafe even before a new claim.
    if reclaim:
        current = await claim(chancy, worker_no_start, queue)
        assert current.claim_id not in (None, old.claim_id)
    expected = await chancy.get_job(ref)
    assert expected.max_attempts == old.max_attempts + 1

    transactional = AsyncMock()
    committed = AsyncMock()
    workflow = chancy.plugins[WorkflowPlugin.get_identifier()]
    monkeypatch.setattr(
        workflow, "on_jobs_updated_in_transaction", transactional
    )
    monkeypatch.setattr(workflow, "on_job_updated", committed)
    await worker_no_start.queue_update(
        replace(old, state=state, attempts=1, meta={"stale": True})
    )
    await worker_no_start.flush()
    assert await chancy.get_job(ref) == expected
    transactional.assert_not_awaited()
    committed.assert_not_awaited()
    assert not worker_no_start._pending_updates

    if reclaim:
        update = replace(current, state=QueuedJob.State.SUCCEEDED, attempts=1)
        await worker_no_start.queue_update(update)
        await worker_no_start.flush()
        assert await chancy.get_job(ref) == update
        assert transactional.await_args.kwargs["jobs"] == (update,)
        committed.assert_awaited_once_with(worker=worker_no_start, job=update)


@pytest.mark.asyncio
async def test_mixed_batch_filters_stale_updates_and_preserves_hook_order(
    chancy, worker_no_start, monkeypatch
):
    queue = await chancy.declare(Queue("default"))
    refs = [
        await chancy.push(Job(func="unused", unique_key=key))
        for key in ("b", "a")
    ]
    async with chancy.pool.connection() as conn:
        old, _ = await worker_no_start.fetch_jobs(queue, conn, up_to=2)
    assert await recover(chancy, worker_no_start) == 2
    current = [await chancy.get_job(ref) for ref in refs]
    updates = [
        current[0].with_meta({"revision": 1}),
        current[1],
        current[0].with_meta({"revision": 2}),
    ]
    workflow = chancy.plugins[WorkflowPlugin.get_identifier()]
    transactional, committed = AsyncMock(), AsyncMock()
    monkeypatch.setattr(
        workflow, "on_jobs_updated_in_transaction", transactional
    )
    monkeypatch.setattr(workflow, "on_job_updated", committed)
    for update in (updates[0], old, updates[1], updates[2]):
        await worker_no_start.queue_update(update)
    await worker_no_start.flush()
    assert transactional.await_args.kwargs["jobs"] == tuple(updates)
    assert [call.kwargs["job"] for call in committed.await_args_list] == updates
    assert (await chancy.get_job(refs[0])).meta == {"revision": 2}


@pytest.mark.asyncio
@pytest.mark.parametrize("use_cursor", [False, True])
@pytest.mark.parametrize("cancelled", [False, True])
async def test_cancellation_fences_execution_completion(
    chancy, worker_no_start, monkeypatch, use_cursor, cancelled
):
    queue = await chancy.declare(Queue("default"))
    ref = await chancy.push(Job(func="unused", max_attempts=3))
    old = await claim(chancy, worker_no_start, queue)
    assert old.claim_id is not None

    if use_cursor:
        async with (
            chancy.pool.connection() as conn,
            conn.cursor(row_factory=dict_row) as cursor,
        ):
            assert await chancy.cancel_job_ex(cursor, ref) == 1
    else:
        await chancy.cancel_job(ref)
    expected = await chancy.get_job(ref)
    assert expected.state == QueuedJob.State.FAILED

    workflow = chancy.plugins[WorkflowPlugin.get_identifier()]
    transactional, committed = AsyncMock(), AsyncMock()
    monkeypatch.setattr(
        workflow, "on_jobs_updated_in_transaction", transactional
    )
    monkeypatch.setattr(workflow, "on_job_updated", committed)
    # Exercise real completion handling: success, or cancellation with
    # attempts remaining (which would otherwise schedule a retry).
    await AsyncExecutor(worker_no_start, queue).on_job_completed(
        job=old, exc=asyncio.CancelledError() if cancelled else None
    )
    await worker_no_start.flush()
    assert await chancy.get_job(ref) == expected
    assert expected.claim_id is None
    assert expected.completed_at is not None
    transactional.assert_not_awaited()
    committed.assert_not_awaited()


@pytest.mark.asyncio
async def test_manual_retry_invalidates_completed_claim(
    chancy, worker_no_start
):
    queue = await chancy.declare(Queue("default"))
    ref = await chancy.push(Job(func="unused"))
    old = await claim(chancy, worker_no_start, queue)
    await worker_no_start.queue_update(
        replace(old, state=QueuedJob.State.FAILED, attempts=1)
    )
    await worker_no_start.flush()
    await chancy.retry_jobs([ref])
    expected = await chancy.get_job(ref)
    assert expected.claim_id is None
    await worker_no_start.queue_update(
        replace(old, state=QueuedJob.State.SUCCEEDED)
    )
    await worker_no_start.flush()
    assert await chancy.get_job(ref) == expected


@pytest.mark.asyncio
async def test_cancelled_execution_cannot_fail_its_replacement(
    chancy, worker_no_start
):
    queue = await chancy.declare(
        Queue("default", executor=chancy.Executor.Async)
    )
    ref = await chancy.push(Job.from_func(wait_until_cancelled))
    old = await claim(chancy, worker_no_start, queue)
    executor = AsyncExecutor(worker_no_start, queue)
    worker_no_start._executors[queue.name] = executor
    try:
        await executor.push(old)
        tasks = list(executor.jobs)
        await asyncio.sleep(0)  # Enter the job wrapper's cancellation boundary.
        assert await recover(chancy, worker_no_start) == 1
        replacement = await claim(chancy, worker_no_start, queue)
        await worker_no_start._handle_recovery(
            Event("job.recovered", {"j": str(old.id), "c": str(old.claim_id)})
        )
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=2)
        assert not worker_no_start.outgoing.empty()
        await worker_no_start.flush()
        assert await chancy.get_job(ref) == replacement
    finally:
        await executor.stop()


@pytest.mark.asyncio
async def test_recovery_notification_commits_with_claim_invalidation(
    chancy, worker_no_start
):
    queue = await chancy.declare(Queue("default"))
    ref = await chancy.push(Job(func="unused"))
    old = await claim(chancy, worker_no_start, queue)
    async with await AsyncConnection.connect(
        chancy.dsn, autocommit=True
    ) as listener:
        await listener.execute(
            sql.SQL("LISTEN {}").format(
                sql.Identifier(f"{chancy.prefix}events")
            )
        )
        with pytest.raises(RuntimeError, match="rollback"):
            async with (
                chancy.pool.connection() as conn,
                conn.cursor(row_factory=dict_row) as cursor,
            ):
                assert (
                    await Recovery.recover(worker_no_start, chancy, cursor) == 1
                )
                raise RuntimeError("rollback")
        assert await chancy.get_job(ref) == old
        assert await recover(chancy, worker_no_start) == 1
        async with chancy.pool.connection() as conn, conn.cursor() as cursor:
            await chancy.notify(cursor, "test.barrier", {})
        received = [
            json.loads(message.payload)
            async for message in listener.notifies(timeout=5, stop_after=2)
        ]
    assert received == [
        {"t": "job.recovered", "j": str(old.id), "c": str(old.claim_id)},
        {"t": "test.barrier"},
    ]


@pytest.mark.asyncio
async def test_recovery_ignores_live_workers_and_locked_jobs(
    chancy, worker_no_start
):
    queue = await chancy.declare(Queue("default"))
    ref = await chancy.push(Job(func="unused"))
    old = await claim(chancy, worker_no_start, queue)
    async with chancy.pool.connection() as conn:
        await conn.execute(
            sql.SQL("SELECT id FROM {} WHERE id = %s FOR UPDATE").format(
                sql.Identifier(f"{chancy.prefix}jobs")
            ),
            [ref.identifier],
        )
        async with asyncio.timeout(2):
            assert await recover(chancy, worker_no_start) == 0
    async with chancy.pool.connection() as conn:
        await worker_no_start.announce_worker(conn)
    assert await recover(chancy, worker_no_start) == 0
    assert await chancy.get_job(ref) == old


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "executor_class", [AsyncExecutor, ThreadedExecutor, ProcessExecutor]
)
async def test_recovery_cancels_only_the_old_execution(
    chancy_just_app, executor_class, monkeypatch
):
    # Both executions can coexist in one executor after reconnection. Put the
    # replacement first to catch accidental cancellation by job ID alone.
    worker = Worker(chancy_just_app, register_signal_handlers=False)
    old = QueuedJob(
        id=uuid4(),
        claim_id=uuid4(),
        func="unused",
        created_at=Job(func="unused").scheduled_at,
    )
    new = replace(old, claim_id=uuid4())
    executor = object.__new__(executor_class)
    make_future = (
        asyncio.get_running_loop().create_future
        if executor_class is AsyncExecutor
        else Future
    )
    newer, abandoned = make_future(), make_future()
    executor.jobs = {newer: new, abandoned: old}
    kill = Mock()
    if executor_class is ProcessExecutor:
        abandoned.set_running_or_notify_cancel()
        executor.pids_for_job = {new.claim_id: 101, old.claim_id: 102}
        executor.pending_cancellations = {}

        def check_cancel_requested(pid, signum):
            assert executor.pending_cancellations == {old.claim_id: True}

        kill.side_effect = check_cancel_requested
        monkeypatch.setattr("chancy.executors.process.os.kill", kill)
    worker._executors["default"] = executor
    event = Event("job.recovered", {"j": str(old.id), "c": str(old.claim_id)})
    await worker._handle_recovery(event)
    assert not newer.cancelled()
    if executor_class is ProcessExecutor:
        if executor.supports(executor.Capability.CANCELLATION):
            kill.assert_called_once()
            assert kill.call_args.args[0] == 102
            for error in (ProcessLookupError, PermissionError, OSError):
                kill.side_effect = error
                await executor.cancel_execution(old)
            kill.side_effect = None
    else:
        assert abandoned.cancelled()

    # A delayed duplicate must not cancel a replacement after the old
    # execution has left the executor.
    del executor.jobs[abandoned]
    await worker._handle_recovery(event)
    assert not newer.cancelled()
    await executor.cancel(Reference(new.id))
    assert newer.cancelled()


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["get_running_jobs", "cancel_execution"])
@pytest.mark.parametrize(
    "error", [BrokenPipeError, EOFError, RuntimeError, asyncio.CancelledError]
)
async def test_recovery_isolates_executor_errors(
    chancy_just_app, caplog, method, error
):
    worker = Worker(chancy_just_app, register_signal_handlers=False)
    job = QueuedJob(
        id=uuid4(),
        claim_id=uuid4(),
        func="unused",
        created_at=Job(func="unused").scheduled_at,
    )
    broken, healthy = Mock(), Mock()
    for executor in (broken, healthy):
        executor.get_running_jobs.return_value = [job]
        executor.cancel_execution = AsyncMock()
    failure = error("executor unavailable")
    getattr(broken, method).side_effect = failure
    worker._executors = {"broken": broken, "healthy": healthy}
    worker.hub.on("job.recovered", worker._handle_recovery)
    subsequent_handler = AsyncMock()
    worker.hub.on("job.recovered", subsequent_handler)
    event = {"j": str(job.id), "c": str(job.claim_id)}

    if error is asyncio.CancelledError:
        with pytest.raises(asyncio.CancelledError):
            await worker.hub.emit("job.recovered", event)
        healthy.cancel_execution.assert_not_awaited()
        subsequent_handler.assert_not_awaited()
        assert not caplog.records
    else:
        await worker.hub.emit("job.recovered", event)
        healthy.cancel_execution.assert_awaited_once_with(job)
        subsequent_handler.assert_awaited_once()
        assert any(
            record.exc_info and record.exc_info[1] is failure
            for record in caplog.records
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "executor_class", [AsyncExecutor, ThreadedExecutor, ProcessExecutor]
)
async def test_cancellation_cancels_all_matching_executions(
    chancy_just_app, executor_class, monkeypatch
):
    worker = Worker(chancy_just_app, register_signal_handlers=False)
    old = QueuedJob(
        id=uuid4(),
        claim_id=uuid4(),
        func="unused",
        created_at=Job(func="unused").scheduled_at,
    )
    replacement = replace(old, claim_id=uuid4())
    unrelated = replace(old, id=uuid4(), claim_id=uuid4())
    executor = object.__new__(executor_class)
    make_future = (
        asyncio.get_running_loop().create_future
        if executor_class is AsyncExecutor
        else Future
    )
    futures = [make_future() for _ in range(3)]
    executor.jobs = dict(zip(futures, (old, unrelated, replacement)))
    kill = Mock()
    if executor_class is ProcessExecutor:
        if not executor.supports(executor.Capability.CANCELLATION):
            pytest.skip("Process cancellation requires SIGUSR1")
        for future in futures:
            future.set_running_or_notify_cancel()
        executor.pids_for_job = {
            old.claim_id: 101,
            unrelated.claim_id: 102,
            replacement.claim_id: 103,
        }
        executor.pending_cancellations = {}
        monkeypatch.setattr("chancy.executors.process.os.kill", kill)
    worker._executors["default"] = executor

    await worker._handle_cancellation(
        Event("job.cancelled", {"j": str(old.id)})
    )

    if executor_class is ProcessExecutor:
        assert [call.args[0] for call in kill.call_args_list] == [101, 103]
    else:
        assert [future.cancelled() for future in futures] == [True, False, True]


@pytest.mark.asyncio
async def test_process_recovery_cancellation_during_child_startup():
    if not ProcessExecutor.supports(ProcessExecutor.Capability.CANCELLATION):
        pytest.skip("Process cancellation requires SIGUSR1")
    old = QueuedJob(
        id=uuid4(),
        claim_id=uuid4(),
        func="unused",
        created_at=Job(func="unused").scheduled_at,
    )
    new = replace(old, claim_id=uuid4())
    executor = object.__new__(ProcessExecutor)
    future = Future()
    future.set_running_or_notify_cancel()
    executor.jobs = {future: old}
    executor.pids_for_job = {new.claim_id: 101}
    executor.pending_cancellations = {}
    await executor.cancel_execution(old)
    assert executor.pending_cancellations == {old.claim_id: True}
    with pytest.raises(asyncio.CancelledError):
        ProcessExecutor.job_wrapper(
            old, executor.pids_for_job, executor.pending_cancellations
        )
    assert executor.pending_cancellations == {}
    assert executor.pids_for_job == {new.claim_id: 101}


@pytest.mark.asyncio
async def test_process_cancellation_signals_preserve_pool(chancy_just_app):
    if not ProcessExecutor.supports(ProcessExecutor.Capability.CANCELLATION):
        pytest.skip("Process cancellation requires SIGUSR1")
    worker = Worker(chancy_just_app, register_signal_handlers=False)
    executor = ProcessExecutor(worker, Queue("default", concurrency=1))

    async def run(func, **kwargs):
        job = QueuedJob(
            id=uuid4(),
            claim_id=uuid4(),
            func=Job.from_func(func).func,
            kwargs=kwargs,
            created_at=Job(func="unused").scheduled_at,
        )
        future = executor.pool.submit(
            executor.job_wrapper,
            job,
            executor.pids_for_job,
            executor.pending_cancellations,
        )
        return await asyncio.wait_for(asyncio.wrap_future(future), timeout=10)

    try:
        _, pid = await run(os.getpid)
        # A delayed signal must be harmless after the wrapper has returned.
        os.kill(pid, signal.SIGUSR1)
        assert (await run(os.getpid))[1] == pid

        # Simulate delivery after this child has moved to another execution.
        old_claim = uuid4()
        executor.pending_cancellations[old_claim] = True
        assert (await run(receive_process_signal))[1] == pid

        # A signal for the active execution still cancels it, and the pool
        # child survives to run another job.
        with pytest.raises(asyncio.CancelledError):
            await run(
                receive_process_signal, requests=executor.pending_cancellations
            )
        assert (await run(os.getpid))[1] == pid
        assert executor.pids_for_job.copy() == {}
        assert executor.pending_cancellations.copy() == {old_claim: True}
    finally:
        await executor.stop()


@pytest.mark.asyncio
async def test_claim_migration_preserves_existing_jobs(chancy):
    ref = await chancy.push(Job(func="unused", kwargs={"preserved": True}))
    before = await chancy.get_job(ref)
    migrator = Migrator("chancy", "chancy.migrations", prefix=chancy.prefix)
    async with chancy.pool.connection() as conn:
        await migrator.migrate(conn, to_version=7)
        await migrator.migrate(conn)
    assert await chancy.get_job(ref) == before
