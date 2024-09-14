import asyncio
from functools import wraps


def run_async_command(f):
    """
    Decorator to run a coroutine as a Click command.
    """

    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper
