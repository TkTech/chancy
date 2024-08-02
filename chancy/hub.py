import inspect
from typing import Callable, Awaitable


class Hub:
    """
    An event hub for registering and emitting events.
    """

    def __init__(self):
        self._handlers: dict[str, list[Callable[..., Awaitable[None]]]] = {}

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

    def off(self, event: str, callback: Callable[..., Awaitable[None]]):
        """
        Remove a callback from an event.

        :param event: The event to remove the callback from.
        :param callback: The callback to remove.
        """
        self._handlers[event] = [
            h for h in self._handlers[event] if h != callback
        ]
