import asyncio
from contextlib import asynccontextmanager, suppress
from dataclasses import replace
from unittest.mock import AsyncMock, Mock

import pytest
import pytest_asyncio
from psycopg import InterfaceError, OperationalError, sql
from psycopg.rows import dict_row

from chancy import Chancy, Queue, QueuedJob, Reference, Worker
from chancy.hub import Event
from chancy.plugins.workflow import EmptyWorkflowError, Workflow, WorkflowPlugin
from chancy.utils import chancy_uuid
from tests.plugins.test_workflow import sync_success


@pytest_asyncio.fixture
async def scheduler(chancy: Chancy):
    plugin = WorkflowPlugin()
    chancy.plugins[plugin.get_identifier()] = plugin
    await plugin.migrate(chancy)
    await chancy.declare(Queue("default", state=Queue.State.PAUSED))
    return plugin


def notify(plugin, worker, workflow_id, *, created=False):
    plugin._on_workflow_event(
        Event(
            "workflow.created" if created else "workflow.step_completed",
            {"id" if created else "workflow_id": str(workflow_id)},
        ),
        worker,
    )


async def process_batch(plugin, chancy, worker, *, poll=False):
    async with (
        chancy.pool.connection() as conn,
        conn.cursor(row_factory=dict_row) as cursor,
    ):
        method = plugin.poll if poll else plugin.process_pending
        return await method(worker, chancy, cursor)


@asynccontextmanager
async def run_scheduler(plugin, chancy, worker):
    task = asyncio.create_task(plugin.run(worker, chancy))
    try:
        await asyncio.sleep(0)  # Register the event handlers.
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


async def wait_for_states(chancy, ids, state):
    async with asyncio.timeout(5):
        while True:
            workflows = await WorkflowPlugin.fetch_workflows(
                chancy, ids=ids, limit=len(ids)
            )
            if len(workflows) == len(ids) and all(
                workflow.state == state for workflow in workflows
            ):
                return workflows
            await asyncio.sleep(0.01)


def test_empty_workflow_validation_allows_building_before_submission():
    workflow = Workflow("empty")
    with pytest.raises(EmptyWorkflowError, match="at least one step"):
        workflow.validate()
    workflow.add("step", sync_success)
    workflow.validate()


@pytest.mark.asyncio
@pytest.mark.parametrize("use_cursor", [False, True])
async def test_empty_workflow_submission_writes_nothing(
    chancy, scheduler, use_cursor
):
    workflow = Workflow("empty")
    if use_cursor:
        async with (
            chancy.pool.connection() as conn,
            conn.cursor(row_factory=dict_row) as cursor,
        ):
            # Catch inside the transaction so rollback cannot hide any writes.
            with pytest.raises(EmptyWorkflowError, match="at least one step"):
                await scheduler.push_ex(cursor, chancy, workflow)
    else:
        with pytest.raises(EmptyWorkflowError, match="at least one step"):
            await scheduler.push(chancy, workflow)

    assert await scheduler.fetch_workflow(chancy, workflow.id) is None
    workflow.add("step", sync_success)
    await scheduler.push(chancy, workflow)
    saved = await scheduler.fetch_workflow(chancy, workflow.id)
    assert list(saved.steps) == ["step"]


@pytest.mark.asyncio
@pytest.mark.parametrize("poll", [False, True], ids=["notification", "poll"])
async def test_legacy_empty_workflows_do_not_block_batch(
    chancy, scheduler, worker_no_start, poll
):
    worker_no_start.is_leader.set()
    empty_ids = {state: chancy_uuid() for state in Workflow.State}
    async with (
        chancy.pool.connection() as conn,
        conn.cursor(row_factory=dict_row) as cursor,
    ):
        # Simulate empty workflows saved before submission validation existed.
        await cursor.executemany(
            sql.SQL(
                "INSERT INTO {workflows} (id, name, state) VALUES (%s, %s, %s)"
            ).format(workflows=sql.Identifier(f"{chancy.prefix}workflows")),
            [
                (wid, "legacy_empty", state.value)
                for state, wid in empty_ids.items()
            ],
        )

    before = {
        workflow.id: workflow
        for workflow in await scheduler.fetch_workflows(chancy)
    }
    assert len(before) == len(empty_ids)
    assert all(workflow.steps == {} for workflow in before.values())
    valid_id = await scheduler.push(
        chancy, Workflow("valid").add("step", sync_success)
    )
    if not poll:
        for workflow_id in [*empty_ids.values(), valid_id]:
            notify(scheduler, worker_no_start, workflow_id, created=True)

    assert (
        await process_batch(scheduler, chancy, worker_no_start, poll=poll) == 3
    )
    for state, workflow_id in empty_ids.items():
        saved = await scheduler.fetch_workflow(chancy, workflow_id)
        assert saved.steps == {}
        if state in (Workflow.State.PENDING, Workflow.State.RUNNING):
            assert saved.state == Workflow.State.FAILED
        else:
            assert saved == before[workflow_id]

    valid = await scheduler.fetch_workflow(chancy, valid_id)
    assert valid.state == Workflow.State.RUNNING
    assert valid.steps["step"].job_id is not None

    # Subsequent batches still make progress and leave terminal empties alone.
    async with chancy.pool.connection() as conn:
        await conn.execute(
            sql.SQL(
                "UPDATE {jobs} SET state = 'succeeded' WHERE id = %s"
            ).format(jobs=sql.Identifier(f"{chancy.prefix}jobs")),
            [valid.steps["step"].job_id],
        )
    if not poll:
        for workflow_id in [*empty_ids.values(), valid_id]:
            notify(scheduler, worker_no_start, workflow_id)
    assert (
        await process_batch(scheduler, chancy, worker_no_start, poll=poll) == 1
    )
    await wait_for_states(chancy, [valid_id], Workflow.State.COMPLETED)


def test_event_queue_is_bounded_and_coalesces(chancy_just_app):
    plugin = WorkflowPlugin(max_pending_workflows=2)
    worker = Worker(chancy_just_app)
    first, second, overflow = [chancy_uuid() for _ in range(3)]
    notify(plugin, worker, first)
    assert not plugin._pending_workflows  # Followers do no event-driven work.

    worker.is_leader.set()
    notify(plugin, worker, first, created=True)
    for _ in range(100):
        notify(plugin, worker, first)
    notify(plugin, worker, second)
    notify(plugin, worker, overflow)
    notify(plugin, worker, first)
    assert list(plugin._pending_workflows) == [first, second]
    assert plugin.wakeup_signal.is_set()


@pytest.mark.asyncio
async def test_notified_workflows_are_loaded_in_batches(
    chancy, scheduler, worker_no_start, monkeypatch
):
    worker_no_start.is_leader.set()
    scheduler.max_workflows_per_run = 2
    ids = [
        await WorkflowPlugin.push(
            chancy, Workflow(f"batch_{i}").add("step", sync_success)
        )
        for i in range(3)
    ]
    fetch = AsyncMock(wraps=scheduler.fetch_workflows_ex)
    monkeypatch.setattr(scheduler, "fetch_workflows_ex", fetch)
    for workflow_id in ids:
        for _ in range(10):
            notify(scheduler, worker_no_start, workflow_id)

    assert await process_batch(scheduler, chancy, worker_no_start) == 2
    assert fetch.await_count == 1
    assert len(fetch.call_args.kwargs["ids"]) == 2
    assert list(scheduler._pending_workflows) == ids[2:]
    assert await process_batch(scheduler, chancy, worker_no_start) == 1
    await wait_for_states(chancy, ids, Workflow.State.RUNNING)

    # Notifications for deleted or terminal workflows are harmless.
    terminal = await WorkflowPlugin.fetch_workflow(chancy, ids[0])
    terminal.state = Workflow.State.COMPLETED
    await WorkflowPlugin.push(chancy, terminal)
    notify(scheduler, worker_no_start, ids[0])
    notify(scheduler, worker_no_start, chancy_uuid())
    assert await process_batch(scheduler, chancy, worker_no_start) == 0
    assert fetch.await_count == 2


@pytest.mark.asyncio
async def test_notifications_during_processing_are_retained(
    chancy, scheduler, worker_no_start, monkeypatch
):
    worker_no_start.is_leader.set()
    workflow_id = await WorkflowPlugin.push(
        chancy, Workflow("in_flight").add("step", sync_success)
    )
    process = scheduler.process_workflow

    async def complete_during_processing(cursor, app, workflow, worker):
        notify(scheduler, worker, workflow.id)
        return await process(cursor, app, workflow, worker)

    monkeypatch.setattr(
        scheduler, "process_workflow", complete_during_processing
    )
    notify(scheduler, worker_no_start, workflow_id)
    assert await process_batch(scheduler, chancy, worker_no_start) == 1
    assert list(scheduler._pending_workflows) == [workflow_id]
    monkeypatch.setattr(scheduler, "process_workflow", process)
    assert await process_batch(scheduler, chancy, worker_no_start) == 1
    assert not scheduler._pending_workflows


@pytest.mark.asyncio
async def test_locked_notification_is_recovered_by_polling(
    chancy, scheduler, worker_no_start
):
    worker_no_start.is_leader.set()
    workflow_id = await WorkflowPlugin.push(
        chancy, Workflow("locked").add("step", sync_success)
    )
    async with chancy.pool.connection() as conn, conn.cursor() as cursor:
        await cursor.execute(
            sql.SQL("SELECT id FROM {table} WHERE id = %s FOR UPDATE").format(
                table=sql.Identifier(f"{chancy.prefix}workflows")
            ),
            [workflow_id],
        )
        notify(scheduler, worker_no_start, workflow_id)
        async with asyncio.timeout(5):
            assert await process_batch(scheduler, chancy, worker_no_start) == 0
        assert not scheduler._pending_workflows

    assert (
        await process_batch(scheduler, chancy, worker_no_start, poll=True) == 1
    )
    await wait_for_states(chancy, [workflow_id], Workflow.State.RUNNING)


@pytest.mark.asyncio
async def test_busy_event_queue_does_not_delay_polling(
    chancy, scheduler, worker_no_start, monkeypatch
):
    worker_no_start.is_leader.set()
    scheduler.polling_interval = 0.02
    workflow_id = await WorkflowPlugin.push(
        chancy, Workflow("busy").add("step", sync_success)
    )
    pending = scheduler.process_pending
    poll = scheduler.poll
    batches = 0
    polls = 0
    polled_twice = asyncio.Event()

    async def busy_batch(worker, app, cursor, **kwargs):
        nonlocal batches
        result = await pending(worker, app, cursor, **kwargs)
        batches += 1
        notify(scheduler, worker, workflow_id)
        await asyncio.sleep(0.01)
        return result

    async def record_poll(worker, app, cursor, **kwargs):
        nonlocal polls
        result = await poll(worker, app, cursor, **kwargs)
        polls += 1
        if polls == 2:
            polled_twice.set()
        return result

    monkeypatch.setattr(scheduler, "process_pending", busy_batch)
    monkeypatch.setattr(scheduler, "poll", record_poll)
    async with run_scheduler(scheduler, chancy, worker_no_start):
        await worker_no_start.hub.emit(
            "workflow.created", {"id": str(workflow_id)}
        )
        await asyncio.wait_for(polled_twice.wait(), timeout=5)
        assert batches >= 2
        assert scheduler._pending_workflows


@pytest.mark.asyncio
async def test_event_handler_does_not_wait_for_processing(
    chancy, scheduler, worker_no_start, monkeypatch
):
    worker_no_start.is_leader.set()
    entered = asyncio.Event()
    release = asyncio.Event()
    pending = scheduler.process_pending
    ids = [chancy_uuid(), chancy_uuid()]

    async def blocked_batch(worker, app, cursor, **kwargs):
        entered.set()
        await release.wait()
        return await pending(worker, app, cursor, **kwargs)

    monkeypatch.setattr(scheduler, "process_pending", blocked_batch)
    async with run_scheduler(scheduler, chancy, worker_no_start):
        await worker_no_start.hub.emit("workflow.created", {"id": str(ids[0])})
        await asyncio.wait_for(entered.wait(), timeout=5)
        await asyncio.wait_for(
            worker_no_start.hub.emit(
                "workflow.step_completed", {"workflow_id": str(ids[1])}
            ),
            timeout=1,
        )
        assert list(scheduler._pending_workflows) == ids
        release.set()

    # A restarted scheduler must not accumulate duplicate hub callbacks.
    scheduler._pending_workflows.clear()
    await worker_no_start.hub.emit("workflow.created", {"id": str(ids[0])})
    assert not scheduler._pending_workflows


@pytest.mark.asyncio
@pytest.mark.parametrize("notifications", [True, False])
async def test_polling_recovers_overflow_and_disabled_notifications(
    chancy, scheduler, worker_no_start, notifications
):
    chancy.notifications = notifications
    worker_no_start.is_leader.set()
    scheduler.polling_interval = 0.02
    scheduler.max_pending_workflows = 1
    scheduler.max_workflows_per_run = 1
    ids = [
        await WorkflowPlugin.push(
            chancy, Workflow(f"overflow_{i}").add("step", sync_success)
        )
        for i in range(3)
    ]
    async with run_scheduler(scheduler, chancy, worker_no_start):
        if notifications:
            for workflow_id in ids:
                await worker_no_start.hub.emit(
                    "workflow.created", {"id": str(workflow_id)}
                )
            assert len(scheduler._pending_workflows) == 1
        await wait_for_states(chancy, ids, Workflow.State.RUNNING)


@pytest.mark.asyncio
async def test_active_index_migration_preserves_existing_workflows(
    chancy, scheduler, worker_no_start
):
    await scheduler.migrate(chancy, to_version=2)
    workflow_id = await WorkflowPlugin.push(
        chancy,
        Workflow("existing")
        .add("first", sync_success)
        .add("second", sync_success, ["first"]),
    )
    await process_batch(scheduler, chancy, worker_no_start, poll=True)
    before = await WorkflowPlugin.fetch_workflow(chancy, workflow_id)
    await scheduler.migrate(chancy)
    assert await WorkflowPlugin.fetch_workflow(chancy, workflow_id) == before

    async with chancy.pool.connection() as conn, conn.cursor() as cursor:
        await cursor.execute(
            "SELECT indexdef FROM pg_indexes WHERE indexname = %s",
            [f"{chancy.prefix}workflows_active_id_idx"],
        )
        definition = (await cursor.fetchone())[0]
        assert "(id)" in definition
        assert "WHERE" in definition
        assert "pending" in definition and "running" in definition

    await scheduler.migrate(chancy, to_version=2)
    assert await WorkflowPlugin.fetch_workflow(chancy, workflow_id) == before
    await scheduler.migrate(chancy)

    # Resume the pre-migration workflow, including its existing job reference.
    for step in ("first", "second"):
        workflow = await WorkflowPlugin.fetch_workflow(chancy, workflow_id)
        job = await chancy.get_job(Reference(workflow.steps[step].job_id))
        await worker_no_start.queue_update(
            replace(job, state=QueuedJob.State.SUCCEEDED)
        )
        await worker_no_start.flush()
        await process_batch(scheduler, chancy, worker_no_start, poll=True)
    await wait_for_states(chancy, [workflow_id], Workflow.State.COMPLETED)


@pytest.mark.asyncio
async def test_flush_sends_one_notification_batch_for_terminal_workflows(
    chancy, scheduler, worker_no_start, monkeypatch
):
    ids = [
        await WorkflowPlugin.push(
            chancy,
            Workflow(f"notify_{i}")
            .add("first", sync_success)
            .add("second", sync_success),
        )
        for i in range(3)
    ]
    await process_batch(scheduler, chancy, worker_no_start, poll=True)
    for i, workflow_id in enumerate(ids):
        workflow = await WorkflowPlugin.fetch_workflow(chancy, workflow_id)
        for step in workflow.steps.values():
            job = await chancy.get_job(Reference(step.job_id))
            state = (
                QueuedJob.State.SUCCEEDED,
                QueuedJob.State.FAILED,
                QueuedJob.State.RETRYING,
            )[i]
            await worker_no_start.queue_update(replace(job, state=state))

    sent = []
    notify_many = chancy.notify_many

    async def record_notifications(cursor, events):
        events = list(events)
        sent.append(events)
        await notify_many(cursor, events)

    checkout = Mock(wraps=chancy.pool.connection)
    monkeypatch.setattr(chancy.pool, "connection", checkout)
    monkeypatch.setattr(chancy, "notify_many", record_notifications)
    await worker_no_start.flush()
    checkout.assert_called_once()
    assert sent == [
        [
            ("workflow.step_completed", {"workflow_id": str(workflow_id)})
            for workflow_id in ids[:2]
        ]
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_type", [OperationalError, asyncio.CancelledError]
)
async def test_failed_batch_hook_rolls_back_jobs_and_retains_updates(
    chancy, scheduler, worker_no_start, monkeypatch, error_type
):
    workflow_id = await WorkflowPlugin.push(
        chancy, Workflow("rollback").add("step", sync_success)
    )
    await process_batch(scheduler, chancy, worker_no_start, poll=True)
    workflow = await WorkflowPlugin.fetch_workflow(chancy, workflow_id)
    ref = Reference(workflow.steps["step"].job_id)
    original = await chancy.get_job(ref)
    update = replace(original, state=QueuedJob.State.SUCCEEDED)
    await worker_no_start.queue_update(update)
    hook = scheduler.on_jobs_updated_in_transaction
    committed_hook = AsyncMock()
    monkeypatch.setattr(scheduler, "on_job_updated", committed_hook)

    async def fail_after_notification(**kwargs):
        await hook(**kwargs)
        raise error_type("interrupted before commit")

    with monkeypatch.context() as patch:
        patch.setattr(
            scheduler, "on_jobs_updated_in_transaction", fail_after_notification
        )
        with pytest.raises(error_type):
            await worker_no_start.flush()

    assert (await chancy.get_job(ref)).state == original.state
    assert worker_no_start._pending_updates == [update]
    committed_hook.assert_not_awaited()
    await worker_no_start.flush()
    assert (await chancy.get_job(ref)).state == QueuedJob.State.SUCCEEDED
    committed_hook.assert_awaited_once_with(worker=worker_no_start, job=update)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["poll", "process_pending"])
@pytest.mark.parametrize("error_type", [OperationalError, InterfaceError])
async def test_scheduler_recovers_failed_transactions(
    chancy, scheduler, worker_no_start, monkeypatch, operation, error_type
):
    worker_no_start.is_leader.set()
    worker_no_start.backoff_initial = 0
    scheduler.polling_interval = 0.01 if operation == "poll" else 3600
    workflow_id = await WorkflowPlugin.push(
        chancy, Workflow("reconnect").add("step", sync_success)
    )
    original = getattr(scheduler, operation)
    observations = AsyncMock()
    monkeypatch.setattr(worker_no_start, "increment_counter", observations)
    failed = asyncio.Event()

    async def fail_once(worker, app, cursor, **kwargs):
        result = await original(worker, app, cursor, **kwargs)
        if not failed.is_set():
            failed.set()
            assert not any(
                call.args[0] == "workflow:reconnect:started"
                for call in observations.await_args_list
            )
            raise error_type("transient failure after processing")
        return result

    monkeypatch.setattr(scheduler, operation, fail_once)
    async with run_scheduler(scheduler, chancy, worker_no_start):
        if operation == "process_pending":
            await worker_no_start.hub.emit(
                "workflow.created", {"id": str(workflow_id)}
            )
        await asyncio.wait_for(failed.wait(), timeout=5)
        await wait_for_states(chancy, [workflow_id], Workflow.State.RUNNING)
        async with asyncio.timeout(5):
            while not any(
                call.args[0] == "workflow:reconnect:started"
                for call in observations.await_args_list
            ):
                await asyncio.sleep(0.01)
        assert (
            sum(
                call.args[0] == "workflow:reconnect:started"
                for call in observations.await_args_list
            )
            == 1
        )

    async with chancy.pool.connection() as conn, conn.cursor() as cursor:
        await cursor.execute(
            sql.SQL("SELECT count(*) FROM {}").format(
                sql.Identifier(f"{chancy.prefix}jobs")
            )
        )
        assert (await cursor.fetchone())[0] == 1


@pytest.mark.asyncio
async def test_scheduler_recovers_uncertain_commit(
    chancy, scheduler, worker_no_start, monkeypatch
):
    worker_no_start.is_leader.set()
    worker_no_start.backoff_initial = 0
    scheduler.polling_interval = 0.01
    scheduler.max_workflows_per_run = 1
    ids = [
        await WorkflowPlugin.push(
            chancy, Workflow(f"commit_{i}").add("step", sync_success)
        )
        for i in range(2)
    ]
    connection = chancy.pool.connection
    failed = asyncio.Event()

    @asynccontextmanager
    async def lose_commit_response(*args, **kwargs):
        async with connection(*args, **kwargs) as conn:
            yield conn
        if not failed.is_set():
            # The server committed, but the client cannot know that.
            failed.set()
            raise OperationalError("lost commit response")

    monkeypatch.setattr(chancy.pool, "connection", lose_commit_response)
    async with run_scheduler(scheduler, chancy, worker_no_start):
        await asyncio.wait_for(failed.wait(), timeout=5)
        await wait_for_states(chancy, ids, Workflow.State.RUNNING)

    async with connection() as conn, conn.cursor() as cursor:
        await cursor.execute(
            sql.SQL("SELECT count(*) FROM {}").format(
                sql.Identifier(f"{chancy.prefix}jobs")
            )
        )
        assert (await cursor.fetchone())[0] == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type", [OperationalError, ValueError])
async def test_scheduler_propagates_exhausted_or_non_transient_errors(
    chancy, scheduler, worker_no_start, monkeypatch, error_type
):
    worker_no_start.is_leader.set()
    worker_no_start.backoff_initial = 0
    worker_no_start.backoff_max_retries = 2
    scheduler.polling_interval = 0
    poll = AsyncMock(side_effect=error_type("persistent failure"))
    monkeypatch.setattr(scheduler, "poll", poll)
    with pytest.raises(error_type, match="persistent failure"):
        await asyncio.wait_for(
            scheduler.run(worker_no_start, chancy), timeout=5
        )
    assert poll.await_count == (3 if error_type is OperationalError else 1)
