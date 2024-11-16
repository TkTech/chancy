import asyncio
import datetime
import enum
import inspect
import uuid
import time
import json
import secrets
import itertools
import contextlib
from typing import Iterable, Coroutine


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

    done, pending = await asyncio.wait(
        tasks,
        timeout=seconds,
        return_when=asyncio.FIRST_COMPLETED,
    )

    for task in pending:
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
    mod = __import__(mod_name, fromlist=[func_name])
    return getattr(mod, func_name)


def chancy_uuid() -> str:
    """
    Generate a UUID suitable for use as a job ID.

    .. note::

        It's UUID7, kinda, since the draft keeps changing.

    :return: str
    """
    t = (time.time_ns() // 100) & 0xFFFFFFFFFFFFFF
    rand = secrets.randbits(62)
    uuid = (t << 68) | (7 << 64) | (2 << 62) | rand
    return f"{uuid:032x}"


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


class TaskManager:
    """
    A simple task manager that keeps track of tasks.
    """

    def __init__(self):
        self.tasks: set[asyncio.Task] = set()

    def add(self, task, *, name: str | None = None) -> asyncio.Task:
        """
        Add a task to the manager.
        """
        if not asyncio.iscoroutine(task):
            raise TypeError("Expected a coroutine object.")

        if not isinstance(task, asyncio.Task):
            task = asyncio.create_task(task)

        task.add_done_callback(self.tasks.discard)
        self.tasks.add(task)

        if name:
            task.set_name(name)

        return task

    async def wait_until_complete(self):
        """
        Wait until all tasks are complete, including any that are added
        after this method is called.
        """
        while self.tasks:
            done, self.tasks = await asyncio.wait(
                self.tasks, return_when=asyncio.FIRST_COMPLETED
            )

            for task in done:
                if task.cancelled():
                    continue

                task.result()

    async def cancel_all(self):
        """
        Cancel all tasks and wait for them to complete.
        """
        for task in self.tasks:
            task.cancel()

        await self.wait_until_complete()

    def __len__(self):
        return len(self.tasks)

    def __iter__(self):
        return iter(self.tasks)

    def __contains__(self, task):
        return task in self.tasks

    def __repr__(self):
        return f"<TaskManager tasks={len(self.tasks)}>"
