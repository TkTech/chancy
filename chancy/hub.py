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

    async def emit(self, event: str, *args, **kwargs):
        """
        Emit an event with the given arguments to all registered handlers.
        """
        for handler in self._handlers.get(event, []):
            await handler(*args, **kwargs)
