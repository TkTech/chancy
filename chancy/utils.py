import asyncio
import datetime
import logging
import uuid
import time
import json
import secrets
import itertools
import contextlib


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

        This will not work for objects that are not defined in a module,
        such as lambdas, REPL functions, functions-in-functions, etc...

    :param obj: The object to get the importable name for.
    :return: str
    """
    return f"{obj.__module__}.{obj.__qualname__}"


def import_string(name):
    """
    Import an object from a string.

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
        raise TypeError

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

    The asyncio.TaskGroup is not used here for a couple of reasons, but mostly
    because it's only available in Python 3.11+ and we want to maintain
    compatibility with earlier versions.
    """

    def __init__(self):
        self.tasks: set[asyncio.Task] = set()

    async def add(self, coro, *, name: str | None = None) -> asyncio.Task:
        """
        Add a task to the manager.
        """
        task = asyncio.create_task(coro)
        self.tasks.add(task)
        if name:
            task.set_name(name)
        return task

    async def remove(self, task: asyncio.Task):
        """
        Remove a task from the manager.
        """
        task.cancel()
        self.tasks.remove(task)

    async def run(self, *, logger: logging.Logger):
        """
        Run all tasks until they are complete.
        """
        while self.tasks:
            done, pending = await asyncio.wait(
                self.tasks, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                self.tasks.remove(task)
                try:
                    task.result()
                except asyncio.CancelledError:
                    logger.debug(f"Task {task.get_name()} was cancelled.")

    async def shutdown(self, *, timeout: int | None = None) -> bool:
        """
        Cancel all tasks and wait for them to complete.

        :param timeout: The number of seconds to wait for a clean shutdown
                        before forcing the tasks to stop.
        :return: False if a timeout occurred trying to cancel the tasks, True
                 if shutdown was successful.
        """
        for task in self.tasks:
            task.cancel()

        try:
            await asyncio.wait_for(
                asyncio.gather(*self.tasks, return_exceptions=True),
                timeout=timeout,
            )
        except (asyncio.TimeoutError, TimeoutError):
            self.tasks = {task for task in self.tasks if not task.done()}
            return False
        except asyncio.CancelledError:
            pass

        self.tasks.clear()
        return True
