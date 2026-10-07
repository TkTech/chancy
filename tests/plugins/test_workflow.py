import asyncio
from dataclasses import replace
from uuid import UUID

import pytest
from psycopg import sql
from psycopg.rows import dict_row

from chancy import Chancy, Queue, QueuedJob, Reference, Worker, job
from chancy.plugins.leadership import ImmediateLeadership
from chancy.plugins.workflow import (
    CircularDependencyError,
    InvalidDependencyError,
    Sequence,
    Workflow,
    WorkflowPlugin,
)
from chancy.utils import chancy_uuid


# Test jobs
@job()
def sync_success():
    return "success"


@job()
def sync_failure():
    raise ValueError("Failed job")


@job()
async def async_success():
    return "success"


@job()
async def async_failure():
    raise ValueError("Failed job")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [
                ImmediateLeadership(),
                WorkflowPlugin(),
            ],
            "no_default_plugins": True,
        },
    ],
    indirect=True,
)
async def test_sequential_workflow(chancy: Chancy, worker):
    """
    Test that a simple sequential workflow executes steps in order.
    """
    await chancy.declare(Queue("default"))

    workflow = (
        Workflow("sequential")
        .add("step1", sync_success)
        .add("step2", sync_success, ["step1"])
        .add("step3", sync_success, ["step2"])
    )

    workflow_id = await WorkflowPlugin.push(chancy, workflow)
    result = await WorkflowPlugin.wait_for_workflow(
        chancy, workflow_id, timeout=30
    )

    assert result.state == Workflow.State.COMPLETED
    assert len(result.steps) == 3
    for step in result.steps.values():
        assert step.state == step.state.SUCCEEDED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [ImmediateLeadership(), WorkflowPlugin()],
            "no_default_plugins": True,
        },
    ],
    indirect=True,
)
async def test_parallel_workflow(chancy: Chancy, worker):
    """
    Test that parallel steps can execute concurrently.
    """
    await chancy.declare(Queue("default"))

    workflow = (
        Workflow("parallel")
        .add("setup", sync_success)
        .add_group(
            [
                ("parallel1", sync_success),
                ("parallel2", sync_success),
                ("parallel3", sync_success),
            ],
            ["setup"],
        )
        .add("finish", sync_success, ["parallel1", "parallel2", "parallel3"])
    )

    workflow_id = await WorkflowPlugin.push(chancy, workflow)
    result = await WorkflowPlugin.wait_for_workflow(
        chancy, workflow_id, timeout=30
    )

    assert result.state == Workflow.State.COMPLETED
    assert len(result.steps) == 5
    for step in result.steps.values():
        assert step.state == step.state.SUCCEEDED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [ImmediateLeadership(), WorkflowPlugin()],
            "no_default_plugins": True,
        }
    ],
    indirect=True,
)
async def test_workflow_failure(chancy: Chancy, worker):
    """
    Test that workflow handles step failures correctly.
    """
    await chancy.declare(Queue("default"))

    workflow = (
        Workflow("failing")
        .add("step1", sync_success)
        .add("step2", sync_failure, ["step1"])
        .add("step3", sync_success, ["step2"])
    )

    workflow_id = await WorkflowPlugin.push(chancy, workflow)
    result = await WorkflowPlugin.wait_for_workflow(
        chancy, workflow_id, timeout=30
    )

    assert result.state == Workflow.State.FAILED
    assert result.steps["step1"].state == result.steps["step1"].state.SUCCEEDED
    assert result.steps["step2"].state == result.steps["step2"].state.FAILED
    # Step 3 was never queued due to the failure of a previous step.
    assert result.steps["step3"].state is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [ImmediateLeadership(), WorkflowPlugin()],
            "no_default_plugins": True,
        }
    ],
    indirect=True,
)
async def test_sequence(chancy: Chancy, worker):
    """
    Test that Sequence helper class works correctly.
    """
    await chancy.declare(Queue("default"))

    sequence = Sequence(
        "test_sequence",
        [
            sync_success,
            sync_success,
            sync_success,
        ],
    )

    workflow_id = await sequence.push(chancy)
    result = await WorkflowPlugin.wait_for_workflow(
        chancy, workflow_id, timeout=30
    )

    assert result.state == Workflow.State.COMPLETED
    assert len(result.steps) == 3
    for step in result.steps.values():
        assert step.state == step.state.SUCCEEDED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [ImmediateLeadership(), WorkflowPlugin()],
            "no_default_plugins": True,
        }
    ],
    indirect=True,
)
async def test_workflow_timeout(chancy: Chancy, worker):
    """
    Test that wait_for_workflow respects timeout.
    """
    await chancy.declare(Queue("default"))

    @job()
    async def slow_job():
        await asyncio.sleep(5)

    workflow = Workflow("timeout").add("step1", slow_job)

    workflow_id = await WorkflowPlugin.push(chancy, workflow)

    with pytest.raises(asyncio.TimeoutError):
        await WorkflowPlugin.wait_for_workflow(chancy, workflow_id, timeout=0.1)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [ImmediateLeadership(), WorkflowPlugin()],
            "no_default_plugins": True,
        }
    ],
    indirect=True,
)
async def test_missing_workflow(chancy: Chancy, worker):
    """
    Test that waiting for a non-existent workflow raises KeyError.
    """
    with pytest.raises(KeyError):
        await WorkflowPlugin.wait_for_workflow(chancy, chancy_uuid(), timeout=1)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chancy",
    [{"plugins": [ImmediateLeadership(), WorkflowPlugin()]}],
    indirect=True,
)
async def test_async_workflow(chancy: Chancy, worker):
    """
    Test that workflow handles async jobs correctly.
    """
    await chancy.declare(Queue("default", executor=Chancy.Executor.Async))

    workflow = (
        Workflow("async")
        .add("step1", async_success)
        .add("step2", async_success, ["step1"])
    )

    workflow_id = await WorkflowPlugin.push(chancy, workflow)
    result = await WorkflowPlugin.wait_for_workflow(
        chancy, workflow_id, timeout=30
    )

    assert result.state == Workflow.State.COMPLETED
    assert len(result.steps) == 2
    for step in result.steps.values():
        assert step.state == step.state.SUCCEEDED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [ImmediateLeadership(), WorkflowPlugin()],
            "no_default_plugins": True,
        }
    ],
    indirect=True,
)
async def test_workflow_with_modified_jobs(chancy: Chancy, worker):
    """
    Test that workflow handles modified jobs correctly.
    """
    await chancy.declare(Queue("high_priority"))

    workflow = (
        Workflow("modified_jobs")
        .add(
            "step1",
            sync_success.job.with_queue("high_priority").with_priority(10),
        )
        .add(
            "step2",
            sync_success.job.with_queue("high_priority").with_max_attempts(3),
            ["step1"],
        )
    )

    workflow_id = await WorkflowPlugin.push(chancy, workflow)
    result = await WorkflowPlugin.wait_for_workflow(
        chancy, workflow_id, timeout=30
    )

    assert result.state == Workflow.State.COMPLETED
    assert len(result.steps) == 2
    for step in result.steps.values():
        assert step.state == step.state.SUCCEEDED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [ImmediateLeadership(), WorkflowPlugin()],
            "no_default_plugins": True,
        },
    ],
    indirect=True,
)
async def test_simple_circular_dependency(chancy: Chancy, worker):
    """
    Test that a simple circular dependency (A -> B -> A) is detected.
    """
    await chancy.declare(Queue("default"))

    workflow = (
        Workflow("circular")
        .add("step_a", sync_success, ["step_b"])
        .add("step_b", sync_success, ["step_a"])
    )

    with pytest.raises(CircularDependencyError) as exc_info:
        await WorkflowPlugin.push(chancy, workflow)

    assert "step_a -> step_b -> step_a" in str(
        exc_info.value
    ) or "step_b -> step_a -> step_b" in str(exc_info.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [ImmediateLeadership(), WorkflowPlugin()],
            "no_default_plugins": True,
        },
    ],
    indirect=True,
)
async def test_complex_circular_dependency(chancy: Chancy, worker):
    """
    Test that a complex circular dependency (A -> B -> C -> A) is detected.
    """
    await chancy.declare(Queue("default"))

    workflow = (
        Workflow("circular")
        .add("step_a", sync_success, ["step_c"])
        .add("step_b", sync_success, ["step_a"])
        .add("step_c", sync_success, ["step_b"])
    )

    with pytest.raises(CircularDependencyError) as exc_info:
        await WorkflowPlugin.push(chancy, workflow)

    assert "Circular dependency detected" in str(exc_info.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [ImmediateLeadership(), WorkflowPlugin()],
            "no_default_plugins": True,
        },
    ],
    indirect=True,
)
async def test_self_dependency(chancy: Chancy, worker):
    """
    Test that a self-dependency (A -> A) is detected.
    """
    await chancy.declare(Queue("default"))

    workflow = Workflow("self_dep").add("step_a", sync_success, ["step_a"])

    with pytest.raises(CircularDependencyError) as exc_info:
        await WorkflowPlugin.push(chancy, workflow)

    assert "step_a" in str(exc_info.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [ImmediateLeadership(), WorkflowPlugin()],
            "no_default_plugins": True,
        },
    ],
    indirect=True,
)
async def test_invalid_dependency_reference(chancy: Chancy, worker):
    """
    Test that referencing a non-existent step is detected.
    """
    await chancy.declare(Queue("default"))

    workflow = (
        Workflow("invalid_dep")
        .add("step_a", sync_success)
        .add("step_b", sync_success, ["non_existent_step"])
    )

    with pytest.raises(InvalidDependencyError) as exc_info:
        await WorkflowPlugin.push(chancy, workflow)

    assert "step_b" in str(exc_info.value)
    assert "non_existent_step" in str(exc_info.value)


@pytest.mark.asyncio
async def test_validate_can_be_called_independently():
    """
    Test that validate() can be called independently without pushing.
    """
    # Valid workflow should not raise
    valid_workflow = (
        Workflow("valid")
        .add("step1", sync_success)
        .add("step2", sync_success, ["step1"])
        .add("step3", sync_success, ["step2"])
    )
    valid_workflow.validate()  # Should not raise

    # Invalid workflow should raise
    invalid_workflow = (
        Workflow("invalid")
        .add("step_a", sync_success, ["step_b"])
        .add("step_b", sync_success, ["step_a"])
    )
    with pytest.raises(CircularDependencyError):
        invalid_workflow.validate()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [ImmediateLeadership(), WorkflowPlugin()],
            "no_default_plugins": True,
        },
    ],
    indirect=True,
)
async def test_complex_valid_dag(chancy: Chancy, worker):
    """
    Test that a complex but valid DAG passes validation.
    """
    await chancy.declare(Queue("default"))

    # Create a diamond-shaped DAG: start -> a,b -> c,d -> end
    workflow = (
        Workflow("complex_dag")
        .add("start", sync_success)
        .add_group(
            [("a", sync_success), ("b", sync_success)],
            ["start"],
        )
        .add_group(
            [("c", sync_success), ("d", sync_success)],
            ["a", "b"],
        )
        .add("end", sync_success, ["c", "d"])
    )

    # Should not raise - this is a valid DAG
    workflow_id = await WorkflowPlugin.push(chancy, workflow)
    result = await WorkflowPlugin.wait_for_workflow(
        chancy, workflow_id, timeout=30
    )

    assert result.state == Workflow.State.COMPLETED


async def _poll(plugin: WorkflowPlugin, chancy: Chancy, worker: Worker) -> int:
    async with (
        chancy.pool.connection() as conn,
        conn.cursor(row_factory=dict_row) as cursor,
    ):
        return await plugin.poll(worker, chancy, cursor)


async def _count_running(chancy: Chancy, ids: list[str]) -> int:
    workflows = await WorkflowPlugin.fetch_workflows(
        chancy, ids=ids, limit=len(ids)
    )
    return sum(w.state == Workflow.State.RUNNING for w in workflows)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [
                ImmediateLeadership(),
                WorkflowPlugin(polling_interval=3600),
            ],
            "no_default_plugins": True,
        },
    ],
    indirect=True,
)
async def test_step_completion_notifies_leader(
    chancy: Chancy, worker_no_start: Worker
):
    """A nonleader's committed update advances the workflow via NOTIFY."""
    # Persist job completions manually so only the notification can advance
    # the workflow. The leader's polling interval exceeds the test timeout.
    await chancy.declare(Queue("default", state=Queue.State.PAUSED))
    workflow_id = await WorkflowPlugin.push(
        chancy,
        Workflow("remote_completion")
        .add("first", sync_success)
        .add("second", sync_success, ["first"]),
    )
    plugin = chancy.plugins[WorkflowPlugin.get_identifier()]
    await _poll(plugin, chancy, worker_no_start)
    workflow = await WorkflowPlugin.fetch_workflow(chancy, workflow_id)
    first = await chancy.get_job(Reference(workflow.steps["first"].job_id))
    assert workflow.steps["second"].job_id is None

    # Start listening after creation so workflow.created cannot wake a poll.
    async with Worker(chancy) as leader:
        await asyncio.wait_for(leader.is_leader.wait(), timeout=5)
        received = asyncio.Event()
        advanced = asyncio.Event()
        leader.hub.on("workflow.step_completed", lambda event: received.set())
        leader.hub.on("workflow.updated", lambda event: advanced.set())

        assert not worker_no_start.is_leader.is_set()
        await worker_no_start.queue_update(
            replace(first, state=QueuedJob.State.SUCCEEDED)
        )
        await worker_no_start.flush()
        await asyncio.wait_for(received.wait(), timeout=5)
        await asyncio.wait_for(advanced.wait(), timeout=5)

        workflow = await WorkflowPlugin.fetch_workflow(chancy, workflow_id)
        assert workflow.steps["first"].state == QueuedJob.State.SUCCEEDED
        assert workflow.steps["second"].job_id is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [ImmediateLeadership(), WorkflowPlugin()],
            "no_default_plugins": True,
        },
    ],
    indirect=True,
)
async def test_poll_processes_full_batch(
    chancy: Chancy, worker_no_start: Worker
):
    """
    A single poll processes up to max_workflows_per_run workflows, not just
    the default fetch limit of 100.
    """
    await chancy.declare(Queue("default"))

    ids = [
        await WorkflowPlugin.push(
            chancy, Workflow(f"batch_{i}").add("step", sync_success)
        )
        for i in range(150)
    ]

    plugin = WorkflowPlugin(max_workflows_per_run=1000)
    assert await _poll(plugin, chancy, worker_no_start) == 150
    assert await _count_running(chancy, ids) == 150


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [ImmediateLeadership(), WorkflowPlugin()],
            "no_default_plugins": True,
        },
    ],
    indirect=True,
)
async def test_poll_continues_after_previous_batch(
    chancy: Chancy, worker_no_start: Worker
):
    """
    When more workflows are active than fit in one poll, each poll picks up
    after the previous batch. Otherwise, workflows waiting on long-running
    steps, which don't change when polled, could fill every batch and starve
    the rest.
    """
    await chancy.declare(Queue("default"))

    async def push(name: str) -> str:
        return await WorkflowPlugin.push(
            chancy, Workflow(name).add("step", sync_success)
        )

    # Start two workflows. The worker isn't running, so their steps never
    # finish and polling them again changes nothing.
    waiting = [await push(f"waiting_{i}") for i in range(2)]
    assert await _poll(WorkflowPlugin(), chancy, worker_no_start) == 2

    new = [await push(f"new_{i}") for i in range(2)]

    plugin = WorkflowPlugin(max_workflows_per_run=2)
    await _poll(plugin, chancy, worker_no_start)
    await _poll(plugin, chancy, worker_no_start)
    assert await _count_running(chancy, waiting + new) == 4


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [ImmediateLeadership(), WorkflowPlugin()],
            "no_default_plugins": True,
        },
    ],
    indirect=True,
)
async def test_poll_revisits_workflows_during_continuous_arrivals(
    chancy: Chancy, worker_no_start: Worker
):
    """New arrivals cannot postpone revisiting an older workflow forever."""
    await chancy.declare(Queue("default"))
    initial = [
        await WorkflowPlugin.push(
            chancy, Workflow(f"initial_{i}").add("step", sync_success)
        )
        for i in range(3)
    ]
    plugin = WorkflowPlugin(max_workflows_per_run=2)
    assert await _poll(plugin, chancy, worker_no_start) == 2

    workflow = await WorkflowPlugin.fetch_workflow(chancy, initial[0])
    first = await chancy.get_job(Reference(workflow.steps["step"].job_id))
    await worker_no_start.queue_update(
        replace(first, state=QueuedJob.State.SUCCEEDED)
    )
    await worker_no_start.flush()

    arrivals = []
    for batch in range(3):
        arrivals.extend(
            [
                await WorkflowPlugin.push(
                    chancy,
                    Workflow(f"arrival_{batch}_{i}").add("step", sync_success),
                )
                for i in range(2)
            ]
        )
        assert 0 < await _poll(plugin, chancy, worker_no_start) <= 2

    workflow = await WorkflowPlugin.fetch_workflow(chancy, initial[0])
    assert workflow.state == Workflow.State.COMPLETED
    # A new sweep must also include arrivals, even while older jobs remain
    # running. Waiting for those jobs to finish would starve the new ones.
    workflow = await WorkflowPlugin.fetch_workflow(chancy, arrivals[0])
    assert workflow.state == Workflow.State.RUNNING


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [ImmediateLeadership(), WorkflowPlugin()],
            "no_default_plugins": True,
        },
    ],
    indirect=True,
)
async def test_poll_restarts_when_sweep_tail_finishes(
    chancy: Chancy, worker_no_start: Worker
):
    """Finishing the remaining workflows does not leave a poll idle."""
    await chancy.declare(Queue("default"))
    # Polling follows UUID order, which need not match creation order.
    ids = [
        await WorkflowPlugin.push(
            chancy,
            Workflow(f"tail_{i}", id=str(UUID(int=i))).add(
                "step", sync_success
            ),
        )
        for i in (3, 1, 2)
    ]
    plugin = WorkflowPlugin(max_workflows_per_run=2)
    assert await _poll(plugin, chancy, worker_no_start) == 2

    tail = await WorkflowPlugin.fetch_workflow(chancy, max(ids))
    tail.state = Workflow.State.COMPLETED
    await WorkflowPlugin.push(chancy, tail)

    assert await _poll(plugin, chancy, worker_no_start) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [ImmediateLeadership(), WorkflowPlugin()],
            "no_default_plugins": True,
        },
    ],
    indirect=True,
)
async def test_poll_revisits_skipped_locks(
    chancy: Chancy, worker_no_start: Worker
):
    """Locked workflows are skipped and revisited after their release."""
    plugin = WorkflowPlugin(max_workflows_per_run=2)
    assert await _poll(plugin, chancy, worker_no_start) == 0
    await chancy.declare(Queue("default"))
    ids = [
        await WorkflowPlugin.push(
            chancy, Workflow(f"locked_{i}").add("step", sync_success)
        )
        for i in range(3)
    ]

    async with chancy.pool.connection() as conn, conn.cursor() as cursor:
        await cursor.execute(
            sql.SQL(
                "SELECT id FROM {workflows} WHERE id = %s FOR UPDATE"
            ).format(workflows=sql.Identifier(f"{chancy.prefix}workflows")),
            [ids[0]],
        )
        assert await _poll(plugin, chancy, worker_no_start) == 2
        assert await _count_running(chancy, ids) == 2

        await cursor.execute(
            sql.SQL("SELECT id FROM {workflows} FOR UPDATE").format(
                workflows=sql.Identifier(f"{chancy.prefix}workflows")
            )
        )
        assert await _poll(plugin, chancy, worker_no_start) == 0

    assert await _poll(plugin, chancy, worker_no_start) == 2
    assert await _count_running(chancy, ids) == 3
