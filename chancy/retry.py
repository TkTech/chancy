import asyncio
import logging
from collections.abc import Callable

from psycopg import InterfaceError, OperationalError


def _raise_if_cancelling() -> None:
    """
    Re-raise a cancellation that was converted into a database error.

    Psycopg can replace a ``CancelledError`` with an ``OperationalError``, for
    example when a cancellation interrupts a pipelined ``executemany()``. Our
    maintenance loops would treat it as a transient error and keep running,
    leaving shutdown waiting forever on the cancelled task.
    """
    task = asyncio.current_task()
    if task is not None and task.cancelling():
        raise asyncio.CancelledError()


class DatabaseRetry:
    """Backoff state for one database maintenance loop."""

    errors = (OperationalError, InterfaceError)

    def __init__(
        self,
        name: str,
        *,
        calculate_delay: Callable[[int], float],
        max_retries: int | None,
        log: logging.Logger,
    ):
        self.name = name
        self.calculate_delay = calculate_delay
        self.max_retries = max_retries
        self.log = log
        self.failures = 0

    def reset(self):
        """Record successful database work."""
        self.failures = 0

    async def wait(self, error: BaseException):
        """Back off after a transient failure, or propagate an exhausted retry."""
        _raise_if_cancelling()
        if not isinstance(error, self.errors):
            raise error
        self.failures += 1
        if self.max_retries is not None and self.failures > self.max_retries:
            raise error
        delay = self.calculate_delay(self.failures - 1)
        self.log.exception(
            "Transient database error in %s, retrying in %.1fs (attempt %s).",
            self.name,
            delay,
            self.failures,
            exc_info=error,
        )
        await asyncio.sleep(delay)
