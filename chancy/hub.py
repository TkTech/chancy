import asyncio
import inspect
from dataclasses import dataclass
from typing import Callable, Awaitable


@dataclass
class Event:
    name: str
    body: dict[str, any]


EventCallbackT = Callable[[Event], Awaitable[None]] | Callable[[Event], None]


class Hub:
    """
    An event hub for registering and emitting events.
    """

    def __init__(self):
        self._handlers: dict[str, list[EventCallbackT]] = {}
        self._waiters: dict[str, set[asyncio.Future]] = {}

    def on(self, event: str, f: EventCallbackT):
        """
        Register a handler for an event.
        """
        self._handlers.setdefault(event, []).append(f)

    def remove(self, event: str, handler: EventCallbackT):
        """
        Remove a handler from an event.
        """
        try:
            self._handlers.get(event, []).remove(handler)
        except ValueError:
            pass

    def clear(self):
        """
        Clear all handlers.
        """
        self._handlers = {}

        for event in self._waiters:
            for waiter in self._waiters[event]:
                if not waiter.done():
                    waiter.cancel()

        self._waiters = {}

    async def emit(self, event: str, body):
        """
        Emit an event with the given body.
        """
        result = Event(name=event, body=body)

        for handler in self._handlers.get(event, []):
            if inspect.iscoroutinefunction(handler):
                await handler(result)
            else:
                handler(result)

        # Notify any asyncio waiters that are waiting for this event, used
        # to implement wait_for().
        waiters = self._waiters.get(event, [])
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(result)

        self._waiters[event] = {w for w in waiters if not w.done()}

    async def wait_for(
        self, event: str | list[str], timeout: float | None = None
    ) -> list[Event]:
        """
        Wait for a specific event to occur.

        :param event: The name of the event to wait for, or a list of events.
        :param timeout: The maximum time to wait for the event (in seconds).
        """
        if isinstance(event, str):
            event = [event]

        if not event:
            raise ValueError("At least one event must be provided.")

        futures = []
        for e in event:
            future = asyncio.get_running_loop().create_future()
            future.add_done_callback(
                lambda _: self._waiters.get(e, set()).discard(future)
            )
            self._waiters.setdefault(e, set()).add(future)
            futures.append(future)

        done, pending = await asyncio.wait(
            futures,
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()

        return [task.result() for task in done]
