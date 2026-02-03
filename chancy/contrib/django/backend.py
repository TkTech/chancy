"""
Django Tasks backend using Chancy as the task queue.

This backend allows Django 6.0+ applications to use Chancy for task
execution via Django's built-in tasks framework.

.. note::

    This is brand new and experimental. Feedback is welcome!

Compared to using Chancy directly, this backend provides limited functionality
as it must conform to the Django Tasks API. However, it allows anything written
against the Django Tasks API to run on Chancy with no code changes.

Configuration example::

    # settings.py
    TASKS = {
        "default": {
            "BACKEND": "chancy.contrib.django.backend.ChancyBackend",
            "QUEUES": ["default"],
            "OPTIONS": {
                "dsn": None,  # Uses DATABASES["default"] if not set
                "prefix": "chancy_",
            },
        },
    }

You'll still need to use the Chancy CLI or library to declare your queues
and run workers, as Django Tasks does not handle that part. See the
Chancy documentation for details.
"""

import inspect
from datetime import datetime, timezone
from functools import cached_property
from typing import TYPE_CHECKING, Any

from asgiref.sync import async_to_sync

from chancy import Chancy
from chancy.job import Job, QueuedJob, Reference
from chancy.utils import importable_name, get_database_dsn

from dataclasses import dataclass, field
from typing import Optional

from django.conf import settings as django_settings
from django.tasks import TaskResult
from django.tasks.base import TaskResultStatus, TaskError
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.exceptions import TaskResultDoesNotExist
from django.utils.module_loading import import_string


if TYPE_CHECKING:
    from django.tasks import Task, TaskResult as DjangoTaskResult


WRAPPER_FUNC = "chancy.contrib.django.task_wrapper.django_task_executor"
ASYNC_WRAPPER_FUNC = (
    "chancy.contrib.django.task_wrapper.async_django_task_executor"
)


@dataclass(frozen=True, kw_only=True)
class ChancyTaskResult(TaskResult):
    """
    A TaskResult subclass that allows setting attempts and return_value directly.
    """

    _attempts: int = field(default=0)
    _return_value: Optional[Any] = field(default=None)

    @property
    def attempts(self):
        return self._attempts


def _build_task_result_from_queued_job(
    job: QueuedJob,
    task: "Task",
) -> "DjangoTaskResult":
    """
    Build a Django TaskResult from a Chancy QueuedJob.

    :param job: The Chancy QueuedJob
    :param task: The Django Task
    :return: A Django TaskResult
    """
    status_map = {
        QueuedJob.State.PENDING: TaskResultStatus.READY,
        QueuedJob.State.RETRYING: TaskResultStatus.READY,
        QueuedJob.State.RUNNING: TaskResultStatus.RUNNING,
        QueuedJob.State.FAILED: TaskResultStatus.FAILED,
        QueuedJob.State.SUCCEEDED: TaskResultStatus.SUCCESSFUL,
    }

    errors = [
        TaskError(
            exception_class_path="Exception",
            traceback=err.get("traceback", ""),
        )
        for err in job.errors
    ]

    args = job.kwargs.get("_args", [])
    task_kwargs = job.kwargs.get("_kwargs", {})

    return ChancyTaskResult(
        task=task,
        id=str(job.id),
        status=status_map.get(job.state, TaskResultStatus.READY),
        enqueued_at=job.created_at,
        started_at=job.started_at,
        finished_at=job.completed_at,
        last_attempted_at=job.started_at,
        args=list(args),
        kwargs=task_kwargs,
        backend="default",
        errors=errors,
        worker_ids=[],
        _attempts=job.attempts + 1,
        _return_value=job.meta.get("_django_return_value"),
    )


class ChancyBackend(BaseTaskBackend):
    """
    A Django Tasks backend that uses Chancy for task queue management.
    """

    supports_defer = True
    supports_async_task = True
    supports_get_result = True
    supports_priority = True

    def __init__(self, alias: str, params: dict[str, Any]):
        super().__init__(alias, params)
        self._default_queue: str = self.options.pop("queue", "default")

    @cached_property
    def task_class(self):
        from django.tasks import Task

        return Task

    @cached_property
    def chancy(self) -> Chancy:
        """Lazy initialization of Chancy instance."""
        dsn = self.options.pop("dsn", None)
        if dsn is None:
            dsn = get_database_dsn(django_settings.DATABASES["default"])
        return Chancy(dsn, **self.options)

    def enqueue(
        self,
        task: "Task",
        args: list[Any],
        kwargs: dict[str, Any],
    ) -> "DjangoTaskResult":
        """
        Enqueue a task for execution.

        :param task: The Task to enqueue
        :param args: Positional arguments for the task
        :param kwargs: Keyword arguments for the task
        :return: A TaskResult for tracking the task
        """
        return async_to_sync(self.aenqueue)(task, args, kwargs)

    async def aenqueue(
        self,
        task: "Task",
        args: list[Any],
        kwargs: dict[str, Any],
    ) -> "DjangoTaskResult":
        """
        Async version of enqueue.

        :param task: The Task to enqueue
        :param args: Positional arguments for the task
        :param kwargs: Keyword arguments for the task
        :return: A TaskResult for tracking the task
        """
        queue_name = getattr(task, "queue_name", None) or self._default_queue

        scheduled_at = datetime.now(tz=timezone.utc)
        run_after = getattr(task, "run_after", None)
        if run_after is not None:
            scheduled_at = run_after

        wrapper_func = (
            ASYNC_WRAPPER_FUNC
            if inspect.iscoroutinefunction(task.func)
            else WRAPPER_FUNC
        )

        job = Job(
            func=wrapper_func,
            queue=queue_name,
            kwargs={
                "_task_path": importable_name(task.func),
                "_args": list(args),
                "_kwargs": kwargs,
                "_takes_context": getattr(task, "takes_context", False),
            },
            priority=getattr(task, "priority", 0),
            scheduled_at=scheduled_at,
        )

        async with self.chancy:
            ref = await self.chancy.push(job)

        return TaskResult(
            task=task,
            id=str(ref.identifier),
            status=TaskResultStatus.READY,
            enqueued_at=datetime.now(tz=timezone.utc),
            started_at=None,
            finished_at=None,
            last_attempted_at=None,
            args=args,
            kwargs=kwargs,
            backend="default",
            errors=[],
            worker_ids=[],
        )

    def get_result(self, result_id: str) -> "DjangoTaskResult":
        """
        Retrieve a task result by ID.

        :param result_id: The task result ID
        :return: The TaskResult
        :raises TaskResultDoesNotExist: If the result doesn't exist
        """
        return async_to_sync(self.aget_result)(result_id)

    async def aget_result(self, result_id: str) -> "DjangoTaskResult":
        """
        Async version of get_result.

        :param result_id: The task result ID
        :return: The TaskResult
        :raises TaskResultDoesNotExist: If the result doesn't exist
        """
        async with self.chancy:
            job = await self.chancy.get_job(Reference(result_id))

        if job is None:
            raise TaskResultDoesNotExist(result_id)

        return _build_task_result_from_queued_job(
            job,
            self.task_class(
                func=import_string(job.kwargs["_task_path"]),
                backend=self.alias,
                queue_name=job.queue,
                priority=job.priority,
                takes_context=job.kwargs.get("_takes_context", False),
                run_after=None,
            ),
        )
