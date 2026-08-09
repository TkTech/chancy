"""
Wrapper function for executing Django Tasks via Chancy.

Since Chancy only supports kwargs, this wrapper receives task metadata
as kwargs and reconstructs the original function call.
"""

from django.tasks import Task, TaskContext
from django.utils.module_loading import import_string

from chancy.contrib.django.backend import _build_task_result_from_queued_job
from chancy.job import QueuedJob


def _build_task_for_context(context: QueuedJob, func) -> Task:
    return Task(
        func=func,
        backend="default",
        queue_name=context.queue,
        priority=context.priority,
        takes_context=True,
        run_after=None,
    )


def django_task_executor(
    *,
    _task_path: str,
    _args: list,
    _kwargs: dict,
    _takes_context: bool = False,
    context: QueuedJob,
):
    """
    Wrapper function that Chancy executes to run Django tasks.

    :param _task_path: Import path to the Django task
    :param _args: Positional arguments to pass to the task
    :param _kwargs: Keyword arguments to pass to the task
    :param _takes_context: Whether the task expects a TaskContext
    :param context: The Chancy QueuedJob, used to build TaskContext
    """
    func = import_string(_task_path)

    if _takes_context:
        task = _build_task_for_context(context, func)
        task_context = TaskContext(
            task_result=_build_task_result_from_queued_job(context, task),
        )
        result = func(task_context, *_args, **_kwargs)
    else:
        result = func(*_args, **_kwargs)

    context.meta["_django_return_value"] = result

    return result


async def async_django_task_executor(
    *,
    _task_path: str,
    _args: list,
    _kwargs: dict,
    _takes_context: bool = False,
    context: QueuedJob,
):
    """
    Async wrapper function that Chancy executes to run async Django tasks.

    :param _task_path: Import path to the Django task
    :param _args: Positional arguments to pass to the task
    :param _kwargs: Keyword arguments to pass to the task
    :param _takes_context: Whether the task expects a TaskContext
    :param context: The Chancy QueuedJob, used to build TaskContext
    """
    func = import_string(_task_path)

    if _takes_context:
        task = _build_task_for_context(context, func)
        task_context = TaskContext(
            task_result=_build_task_result_from_queued_job(context, task),
        )
        result = await func(task_context, *_args, **_kwargs)
    else:
        result = await func(*_args, **_kwargs)

    context.meta["_django_return_value"] = result

    return result
