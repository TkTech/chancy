"""
Tests for Django Tasks backend integration.

These tests require Django 6.0+ which includes the built-in tasks framework.
"""

import pytest
import django

# Skip all tests in this module if Django < 6.0
pytestmark = pytest.mark.skipif(
    django.VERSION < (6, 0),
    reason="Django Tasks API requires Django 6.0+",
)


# Task functions must be defined at module level for Django Tasks
def add(a: int, b: int) -> int:
    return a + b


def greet(name: str, greeting: str = "Hello") -> str:
    return f"{greeting}, {name}!"


def high_priority_task() -> str:
    return "high"


def deferred_task() -> str:
    return "done"


def simple_task() -> int:
    return 42


def task_with_context(context, value: int) -> dict:
    return {
        "value": value,
        "attempt": context.attempt,
        "result_id": context.task_result.id,
    }


def failing_task() -> None:
    raise ValueError("This task always fails")


async def async_add(a: int, b: int) -> int:
    import asyncio

    await asyncio.sleep(0.1)
    return a + b


@pytest.fixture
def django_tasks_settings(settings, chancy):
    """Configure Django Tasks to use the Chancy backend."""
    settings.TASKS = {
        "default": {
            "BACKEND": "chancy.contrib.django.backend.ChancyBackend",
            "QUEUES": ["default", "async_queue"],
            "OPTIONS": {
                "dsn": "postgresql://postgres:localtest@localhost:8190/postgres",
                "prefix": chancy.prefix,
            },
        },
    }
    return settings


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_enqueue_task(chancy, worker, django_tasks_settings):
    """Test that a Django task can be enqueued via the Chancy backend."""
    from django.tasks import task
    from django.tasks.base import TaskResultStatus

    from chancy.job import Reference

    add_task = task(add)

    result = await add_task.aenqueue(2, 3)

    assert result.id is not None
    assert result.status == TaskResultStatus.READY

    queued_job = await chancy.wait_for_job(Reference(result.id), timeout=10)

    assert queued_job is not None
    assert queued_job.state.value == "succeeded"

    await result.arefresh()
    assert result.status == TaskResultStatus.SUCCESSFUL
    assert result.return_value == 5


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_enqueue_task_with_kwargs(chancy, worker, django_tasks_settings):
    """Test that a Django task can be enqueued with keyword arguments."""
    from django.tasks import task

    from chancy.job import Reference, QueuedJob

    greet_task = task(greet)

    result = await greet_task.aenqueue("World", greeting="Hi")

    queued_job = await chancy.wait_for_job(Reference(result.id), timeout=10)
    assert queued_job.state == QueuedJob.State.SUCCEEDED

    await result.arefresh()
    assert result.return_value == "Hi, World!"


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_enqueue_task_with_priority(
    chancy, worker, django_tasks_settings
):
    """Test that task priority is respected."""
    from django.tasks import task

    from chancy.job import Reference

    priority_task = task(priority=10)(high_priority_task)

    result = await priority_task.aenqueue()

    queued_job = await chancy.get_job(Reference(result.id))
    assert queued_job.priority == 10


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_enqueue_deferred_task(chancy, worker, django_tasks_settings):
    """Test that tasks can be deferred using run_after."""
    from datetime import datetime, timedelta, timezone

    from django.tasks import task

    from chancy.job import Reference

    defer_task = task(deferred_task)

    run_after = datetime.now(tz=timezone.utc) + timedelta(seconds=5)
    result = await defer_task.using(run_after=run_after).aenqueue()

    queued_job = await chancy.get_job(Reference(result.id))
    assert queued_job.scheduled_at >= run_after - timedelta(seconds=1)


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_get_result(chancy, worker, django_tasks_settings):
    """Test retrieving a result by ID."""
    from django.tasks import task
    from django.tasks.base import TaskResultStatus

    from chancy.contrib.django.backend import ChancyBackend
    from chancy.job import Reference

    simple = task(simple_task)

    result = await simple.aenqueue()
    result_id = result.id

    await chancy.wait_for_job(Reference(result_id), timeout=10)

    backend = ChancyBackend(
        "default",
        {"OPTIONS": django_tasks_settings.TASKS["default"]["OPTIONS"]},
    )
    retrieved = await backend.aget_result(result_id)

    assert retrieved.id == result_id
    assert retrieved.status == TaskResultStatus.SUCCESSFUL
    assert retrieved.return_value == 42

    # Verify that arefresh() works on retrieved results (requires proper Task object)
    await retrieved.arefresh()
    assert retrieved.status == TaskResultStatus.SUCCESSFUL


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_task_with_context(chancy, worker, django_tasks_settings):
    """Test that tasks with takes_context=True receive a TaskContext."""
    from django.tasks import task
    from django.tasks.base import TaskResultStatus

    from chancy.job import Reference

    context_task = task(takes_context=True)(task_with_context)

    result = await context_task.aenqueue(100)

    await chancy.wait_for_job(Reference(result.id), timeout=10)

    await result.arefresh()
    assert result.status == TaskResultStatus.SUCCESSFUL
    assert result.return_value["value"] == 100
    assert result.return_value["attempt"] == 1
    assert result.return_value["result_id"] == result.id


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_failed_task(chancy, worker, django_tasks_settings):
    """Test that failed tasks are properly tracked."""
    from django.tasks import task
    from django.tasks.base import TaskResultStatus

    from chancy.job import Reference

    fail_task = task(failing_task)

    result = await fail_task.aenqueue()

    await chancy.wait_for_job(Reference(result.id), timeout=10)

    await result.arefresh()
    assert result.status == TaskResultStatus.FAILED
    assert len(result.errors) > 0


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_async_task(chancy, django_tasks_settings):
    """Test that async tasks work correctly."""
    from django.tasks import task
    from django.tasks.base import TaskResultStatus

    from chancy import Queue, Worker
    from chancy.job import Reference

    async_task = task(async_add)

    await chancy.declare(
        Queue(
            name="async_queue",
            executor="chancy.executors.asyncex.AsyncExecutor",
        ),
        upsert=True,
    )

    async with Worker(chancy):
        result = await async_task.using(queue_name="async_queue").aenqueue(5, 7)

        await chancy.wait_for_job(Reference(result.id), timeout=10)

        await result.arefresh()
        assert result.status == TaskResultStatus.SUCCESSFUL
        assert result.return_value == 12
