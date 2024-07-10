import time
import secrets
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
