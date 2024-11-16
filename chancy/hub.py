import asyncio
import inspect
from typing import Callable, Awaitable


class Hub:
    """
    An event hub for registering and emitting events.
    """

    def __init__(self):
        self._handlers: dict[str, list[Callable[..., Awaitable[None]]]] = {}
        self._waiters: dict[str, list[asyncio.Future]] = {}

    def on(self, event: str, f: Callable[..., Awaitable[None]]):
        """
        Register a handler for an event.
        """
        self._handlers.setdefault(event, []).append(f)

    def remove(self, event: str, handler: Callable[..., Awaitable[None]]):
        """
        Remove a handler from an event.
        """
        try:
            self._handlers.get(event, []).remove(handler)
        except ValueError:
            pass

    async def emit(self, event: str, body):
        """
        Emit an event with the given body.
        """
        for handler in self._handlers.get(event, []):
            # Check if the handler is async
            if inspect.iscoroutinefunction(handler):
                await handler(body)
            else:
                handler(body)

        # Emit wildcard handlers.
        if "*" in self._handlers:
            for handler in self._handlers["*"]:
                if inspect.iscoroutinefunction(handler):
                    await handler(event, body)
                else:
                    handler(event, body)

        # Resolve any waiting futures
        waiters = self._waiters.get(event, [])
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(body)
        self._waiters[event] = [w for w in waiters if not w.done()]

    async def wait_for(self, event: str, timeout: float | None = None):
        """
        Wait for a specific event to occur.

        :param event: The event to wait for.
        :param timeout: The maximum time to wait for the event (in seconds).
        """
        future = asyncio.get_running_loop().create_future()
        self._waiters.setdefault(event, []).append(future)

        done, pending = await asyncio.wait(
            [future],
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()

        self._waiters[event].remove(future)
        return
