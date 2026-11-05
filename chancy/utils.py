import asyncio
import contextlib
import datetime
import enum
import importlib
import inspect
import itertools
import json
import logging
import secrets
import time
import uuid
from collections.abc import Callable, Coroutine, Iterable
from dataclasses import asdict, is_dataclass
from typing import Any, NotRequired, TypedDict
from urllib.parse import quote_plus, urlencode, urlunparse


class DatabaseConnection(TypedDict):
    """
    Django-style database connection configuration.
    """

    ENGINE: str
    NAME: NotRequired[str]
    USER: NotRequired[str]
    PASSWORD: NotRequired[str]
    HOST: NotRequired[str]
    PORT: NotRequired[str]
    OPTIONS: NotRequired[dict[str, Any]]


async def sleep(
    seconds: int, *, events: Iterable[Coroutine] | None = None
) -> bool:
    """
    Sleep for a specified number of seconds, or until one of the given events
    occurs.
    """
    if not events:
        await asyncio.sleep(seconds)
        return True

    tasks = [asyncio.create_task(event) for event in events]
    try:
        _done, pending = await asyncio.wait(
            tasks,
            timeout=seconds,
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()

    return True


@contextlib.contextmanager
def timed_block():
    """
    A context manager that times the block of code within it. The elapsed
    time will be updated in the `elapsed` attribute of the returned object
    until the block is exited.

    Example:

    .. code-block:: python

        with timed_block() as timer:
            time.sleep(1)

        print(f"Block took {timer.elapsed} seconds.")
    """

    class Timer:
        def __init__(self):
            self.start = time.monotonic()
            self.stop = None

        @property
        def elapsed(self):
            return (self.stop or time.monotonic()) - self.start

    timer = Timer()
    yield timer
    timer.stop = time.monotonic()


def importable_name(obj):
    """
    Get the importable name for an object.

    .. note::

        This will only work for objects that are actually importable,
        i.e. they are defined in a module. Lambdas, for example, will not
        have an importable name.

    :param obj: The object to get the importable name for.
    :return: str
    """
    if obj.__class__.__module__ == "__main__":
        module = inspect.getmodule(obj)
        return f"{module.__spec__.name}.{obj.__qualname__}"

    return f"{obj.__module__}.{obj.__qualname__}"


def import_string(name):
    """
    Import an object from a string previously created using
    :func:`importable_name`.

    :param name: The importable name of the object.
    :return: Any
    """
    mod_name, _, func_name = name.rpartition(".")
    module = importlib.import_module(mod_name)
    return getattr(module, func_name)


def lock_order_key(unique_key: str | None) -> str:
    """
    The key used to order job rows before locking them.

    Every code path that takes row locks on several jobs in one transaction
    (pushing jobs with unique keys, the worker's batched job updates) must
    sort by this key so that overlapping transactions acquire their locks in
    the same order and cannot deadlock. See issue #89.
    """
    return unique_key or ""


def chancy_uuid() -> uuid.UUID:
    """
    Generate a UUID suitable for use as a job ID.

    .. note::

        It's UUID7, kinda, since the draft keeps changing.

    :return: UUID
    """
    t = (time.time_ns() // 100) & 0xFFFFFFFFFFFFFF
    rand = secrets.randbits(62)
    uuid7 = (t << 68) | (7 << 64) | (2 << 62) | rand
    return uuid.UUID(f"{uuid7:032x}")


def json_dumps(obj, **kwargs):
    """
    Serialize an object to a JSON formatted str with support for UUIDs.

    :param obj: The object to serialize.
    :param kwargs: Additional arguments to pass to `json.dumps`.
    :return: str
    """

    def _dump(o):
        if isinstance(o, uuid.UUID):
            return str(o)
        elif isinstance(o, datetime.datetime):
            return o.isoformat()
        elif isinstance(o, enum.Enum):
            return o.value
        elif is_dataclass(o):
            return asdict(o)
        elif isinstance(o, (set, frozenset)):
            return list(o)

        raise TypeError(
            f"Object of type {o.__class__.__name__} is not JSON serializable"
        )

    return json.dumps(obj, default=_dump, **kwargs)


def chunked(iterable, size):
    """
    Yield chunks of `size` from `iterable`.

    :param iterable: The iterable to chunk.
    :param size: The size of each chunk.
    """
    it = iter(iterable)
    while True:
        chunk = list(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk


class ExceptionSnapshot:
    """Retain a failure's traceback before reporting adds caller frames."""

    def __init__(self, exception: BaseException):
        self.exception = exception
        self.traceback = exception.__traceback__


class TaskManager:
    """
    Track live tasks separately from their failures.

    Owners classify exits with ``on_exit`` and handle failures with ``on_failure``.
    Cancellation is requested once per task; ``cancel_all`` closes admission.
    """

    def __init__(
        self,
        *,
        log: logging.Logger | None = None,
        on_failure: Callable[[], None] | None = None,
        on_exit: Callable[[asyncio.Task, bool], BaseException | None]
        | None = None,
    ):
        self._log = log if log is not None else logging.getLogger(__name__)
        self._on_failure = on_failure
        self._on_exit = on_exit
        self._tasks: set[asyncio.Task] = set()
        self._cancel_requested: set[asyncio.Task] = set()
        self._errors: list[ExceptionSnapshot] = []
        self._changed = asyncio.Event()
        self._closed = False

    @property
    def errors(self) -> tuple[BaseException, ...]:
        """All failures observed so far, in completion order."""
        return tuple(error.exception for error in self._errors)

    def add(self, name: str, task: Coroutine) -> asyncio.Task:
        """
        Add a task to the manager.
        """
        if self._closed:
            task.close()
            raise RuntimeError("Task manager is closed")
        task: asyncio.Task = asyncio.create_task(task, name=name)
        task.add_done_callback(self._task_done)

        self._tasks.add(task)
        self._changed.set()

        return task

    def _task_done(self, task):
        requested = task in self._cancel_requested
        self._tasks.discard(task)
        self._cancel_requested.discard(task)
        self._changed.set()
        error = self._on_exit(task, requested) if self._on_exit else None
        if not task.cancelled():
            task_error = task.exception()
            if task_error is not None:
                error = task_error
        if error is None:
            return
        self._errors.append(ExceptionSnapshot(error))
        self._log.error("Task %r failed", task.get_name(), exc_info=error)
        if self._on_failure is not None:
            self._on_failure()

    async def wait_for_shutdown(self):
        """
        Wait for all tasks, then report the retained failures to every caller.
        """
        while self._tasks:
            self._changed.clear()
            await self._changed.wait()
        self.raise_errors("Background tasks failed")

    def raise_errors(
        self, message: str, errors: Iterable[ExceptionSnapshot] = ()
    ) -> None:
        """Report retained task failures together with any owner failures."""
        unique = {}
        for error in itertools.chain(self._errors, errors):
            unique.setdefault(id(error.exception), error)
        exceptions = [
            error.exception.with_traceback(error.traceback)
            for error in unique.values()
        ]
        if len(exceptions) == 1:
            raise exceptions[0]
        if exceptions:
            raise BaseExceptionGroup(message, exceptions)

    async def cancel_all(self, *, timeout: float | None = None):
        """
        Close admission, cancel tasks once, and wait up to ``timeout`` seconds.

        A timeout leaves unfinished tasks tracked. Calling again waits without
        interrupting their cleanup with another cancellation request.
        """
        self._closed = True
        await self.cancel_tasks(tuple(self._tasks), timeout=timeout)

    def request_cancel_all(self) -> None:
        """Close admission and request cancellation without waiting."""
        self._closed = True
        self._request_cancellation(self._tasks)

    def _request_cancellation(self, tasks: Iterable[asyncio.Task]):
        tasks = set(tasks) & self._tasks
        for task in tasks - self._cancel_requested:
            self._cancel_requested.add(task)
            task.cancel("Cancelled by TaskManager")
        return tasks

    async def cancel_tasks(
        self, tasks: Iterable[asyncio.Task], *, timeout: float | None = None
    ):
        """Cancel owned tasks by handle; names are only diagnostic labels."""
        tasks = self._request_cancellation(tasks)
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=timeout)
            if pending:
                names = ", ".join(sorted(task.get_name() for task in pending))
                raise TimeoutError(f"Tasks did not stop: {names}")

    async def cancel(self, task: asyncio.Task, *, timeout: float | None = None):
        """Cancel one task by handle and wait for it to finish."""
        if not isinstance(task, asyncio.Task):
            raise TypeError("Cancel by task handle, not by name")
        await self.cancel_tasks((task,), timeout=timeout)

    def __len__(self):
        return len(self._tasks)

    def __iter__(self):
        return iter(self._tasks)

    def __contains__(self, task: asyncio.Task):
        return task in self._tasks

    def __repr__(self):
        return f"<{self.__class__.__name__} tasks={len(self._tasks)!r}>"


def get_database_dsn(db_settings: DatabaseConnection) -> str:
    """
    Convert a django-style database connection configuration to a DSN.
    """
    user = db_settings.get("USER", "")
    password = db_settings.get("PASSWORD", "")
    host = db_settings.get("HOST", "")
    port = db_settings.get("PORT", "5432")
    name = db_settings.get("NAME", "")
    options = db_settings.get("OPTIONS", {})

    netloc = ""
    if user:
        netloc = quote_plus(user)
        if password:
            netloc += f":{quote_plus(password)}"
        netloc += "@"

    if host:
        netloc += host
        if port:
            netloc += f":{port}"

    return urlunparse(
        (
            "postgresql",
            netloc,
            f"/{name}" if name else "/",
            "",
            urlencode(options) if options else "",
            "",
        )
    )
