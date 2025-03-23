import asyncio
import pytest

from chancy import Chancy, Queue, job
from chancy.plugins.workflow import Workflow, WorkflowPlugin, Sequence
from chancy.plugins.leadership import ImmediateLeadership
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
