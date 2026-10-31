import asyncio
import enum
import inspect
import logging
import math
import typing
from collections.abc import Awaitable, Callable
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from functools import partial
from typing import Any, Literal, ParamSpec, TypeVar

from psycopg import AsyncCursor, sql
from psycopg.rows import DictRow, dict_row

from chancy.hub import EventCallbackT

if typing.TYPE_CHECKING:
    from chancy.app import Chancy
    from chancy.plugin import Plugin
    from chancy.worker import Worker

T = TypeVar("T")
P = ParamSpec("P")


class Authority(enum.Enum):
    """Choose which workers may start an operation."""

    #: Run on every worker.
    WORKER = "worker"
    #: Wait for leadership and verify the lease in each database transaction.
    LEADER = "leader"


class LeadershipLost(Exception):
    """The worker no longer owns a valid leadership lease."""


class TransactionContext:
    """
    The transaction passed to :meth:`OperationContext.database` callbacks.

    Chancy commits when the callback returns and rolls back when it raises.
    """

    def __init__(self, cursor: AsyncCursor[DictRow], chancy: "Chancy") -> None:
        #: Cursor for this attempt. Do not commit or roll back its connection.
        self.cursor = cursor
        self._chancy = chancy

    async def notify(self, event: str, body: dict[str, Any]) -> None:
        """Send a database notification when this transaction commits."""
        await self._chancy.notify(self.cursor, event, body)


class OperationContext:
    """The context passed to periodic operations and services."""

    def __init__(
        self, session: "PluginSession", operation: "Operation"
    ) -> None:
        #: Worker running the operation.
        self.worker: Worker = session.worker
        #: Chancy application used by the worker.
        self.chancy: Chancy = session.worker.chancy
        #: Application logger.
        self.log: logging.Logger = self.chancy.log
        self._operation = operation

    async def database(
        self, callback: Callable[[TransactionContext], Awaitable[T]]
    ) -> T:
        """
        Run a callback in a database transaction and return its result.

        Transient database errors retry the callback in a fresh transaction,
        using the worker's backoff settings. The callback must be safe to repeat,
        even if an earlier attempt committed but its response was lost. Return
        results from the callback; publish observations after this method returns.
        Cancellation, other errors, and exhausted retries propagate to the caller.

        For leader operations, each transaction verifies the lease and prevents
        takeover until it ends. This does not protect calls to external services.

        :param callback: Async callable accepting a :class:`TransactionContext`.
        :return: The callback's result after a successful commit.
        """
        retry = self.worker.database_retry(self._operation.name)
        while True:
            try:
                async with (
                    self.chancy.pool.connection() as conn,
                    conn.transaction(),
                    conn.cursor(row_factory=dict_row) as cursor,
                ):
                    if self._operation.authority is Authority.LEADER:
                        await self._guard_leadership(cursor)
                    result = await callback(
                        TransactionContext(cursor, self.chancy)
                    )
                return result
            except retry.errors as exc:
                if not self.worker._leadership_unfenced:
                    self.worker.is_leader.clear()
                self._operation.state = "retrying"
                self._operation.error = exc
                await retry.wait(exc)
                self._operation.state = "running"
                self._operation.error = None

    async def _guard_leadership(self, cursor: AsyncCursor[DictRow]) -> None:
        if self.worker._leadership_unfenced:
            if self.worker.is_leader.is_set():
                return
            raise LeadershipLost()
        # Hold the lease against renewal or takeover until this work commits.
        await cursor.execute(
            sql.SQL(
                "SELECT worker_id, expires_at FROM {} WHERE id = 1 FOR SHARE"
            ).format(sql.Identifier(f"{self.chancy.prefix}leader"))
        )
        lease = await cursor.fetchone()
        await cursor.execute("SELECT clock_timestamp() AS now")
        clock = await cursor.fetchone()
        assert clock is not None
        now = clock["now"]
        if (
            lease is None
            or lease["worker_id"] != self.worker.worker_id
            or lease["expires_at"] <= now
        ):
            self.worker.is_leader.clear()
            raise LeadershipLost()


@dataclass
class Operation:
    """A registered operation and its current execution state."""

    name: str
    callback: Callable[[OperationContext], Awaitable[None]]
    interval: float | None
    authority: Authority
    immediate: bool = False
    state: Literal[
        "registered", "waiting", "running", "retrying", "failed", "stopped"
    ] = "registered"
    error: BaseException | None = None
    wakeup: asyncio.Event = field(default_factory=asyncio.Event)

    def wake_up(self) -> None:
        """Request an early run. Repeated requests are combined."""
        self.wakeup.set()


class PluginSession:
    """
    Register work and resources during :meth:`~chancy.plugin.Plugin.setup`.

    Each worker creates its own session. Registered tasks start after all
    plugins finish setup and are cancelled during shutdown. Subscriptions and
    cleanup callbacks remain until final worker hooks finish. Failed setup
    closes the session immediately.
    """

    def __init__(self, plugin: "Plugin", worker: "Worker") -> None:
        self.plugin = plugin
        self.worker = worker
        self.operations: dict[str, Operation] = {}
        self._resources = AsyncExitStack()
        self._started = False
        self._closed = False
        self._tasks: list[asyncio.Task[None]] = []
        self._close_task: asyncio.Task[None] | None = None

    def periodic(
        self,
        name: str,
        callback: Callable[[OperationContext], Awaitable[None]],
        *,
        interval: float,
        authority: Authority = Authority.WORKER,
        immediate: bool = False,
    ) -> Operation:
        """
        Register a callback to run periodically.

        Calls to this operation do not overlap within this session. Early
        wakeups keep the existing deadline; after a scheduled run finishes, the
        next interval begins. Missed intervals do not accumulate.

        Leader operations wait for leadership before each call. Losing leadership
        does not cancel the callback; use :meth:`OperationContext.database` to
        protect database changes. Exceptions outside that method are not retried.

        :param name: Operation name, unique within this session.
        :param callback: Async callable accepting an :class:`OperationContext`.
        :param interval: Seconds between scheduled runs. Zero runs continuously.
        :param authority: Which workers may run the operation.
        :param immediate: Run immediately instead of waiting for the first interval.
        :return: An operation that can be woken early with :meth:`Operation.wake_up`.
        """
        if not math.isfinite(interval) or interval < 0:
            raise ValueError("interval must be finite and non-negative")
        return self._register(name, callback, interval, authority, immediate)

    def service(
        self, name: str, callback: Callable[[OperationContext], Awaitable[None]]
    ) -> Operation:
        """
        Register an async callback to run once when the worker starts.

        The callback receives an :class:`OperationContext`. It may run until
        shutdown cancels it, or return normally. Failed services are not restarted.

        :param name: Operation name, unique within this session.
        :param callback: Async callable implementing the service.
        :return: The registered operation.
        """
        return self._register(name, callback, None, Authority.WORKER, True)

    def _register(
        self,
        name: str,
        callback: Callable[[OperationContext], Awaitable[None]],
        interval: float | None,
        authority: Authority,
        immediate: bool,
    ) -> Operation:
        if self._started or self._closed:
            raise RuntimeError("Operations must be registered during setup")
        if not isinstance(authority, Authority):
            raise TypeError("authority must be an Authority")
        if name in self.operations:
            raise ValueError(f"Duplicate operation {name!r}")
        operation = Operation(name, callback, interval, authority, immediate)
        self.operations[name] = operation
        return operation

    def subscribe(self, event: str | None, callback: EventCallbackT) -> None:
        """
        Register a handler until the session closes.

        :param event: Event name, or None to receive all events.
        :param callback: Sync or async callable accepting a :class:`~chancy.hub.Event`.
        """
        if self._closed:
            raise RuntimeError("Session is closed")
        if event is None:
            self.worker.hub.on_any(callback)
            self._resources.callback(self.worker.hub.remove_on_any, callback)
        else:
            self.worker.hub.on(event, callback)
            self._resources.callback(self.worker.hub.remove, event, callback)

    def callback(
        self, callback: Callable[P, object], *args: P.args, **kwargs: P.kwargs
    ) -> None:
        """
        Register synchronous cleanup, including when setup fails.

        Callbacks run in reverse registration order.

        :param callback: Synchronous callable to invoke when the session closes.
        :param args: Positional arguments for the callback.
        :param kwargs: Keyword arguments for the callback.
        """
        target = callback
        while isinstance(target, partial):
            target = target.func
        if inspect.iscoroutinefunction(target) or inspect.iscoroutinefunction(
            type(target).__call__
        ):
            raise TypeError("Session cleanup callbacks must be synchronous")
        if self._closed:
            raise RuntimeError("Session is closed")

        def cleanup() -> None:
            result = callback(*args, **kwargs)
            if inspect.isawaitable(result):
                if inspect.iscoroutine(result):
                    result.close()
                raise TypeError("Session cleanup callbacks must be synchronous")

        self._resources.callback(cleanup)

    def start(self) -> None:
        """Start registered tasks. Called by the worker after setup."""
        if self._started or self._closed:
            raise RuntimeError("Session has already started or closed")
        self._started = True
        for operation in self.operations.values():
            self._tasks.append(
                self.worker.manager.add(
                    f"{self.plugin.get_identifier()}:{operation.name}",
                    self._run(operation),
                )
            )

    async def _run(self, operation: Operation) -> None:
        ctx = OperationContext(self, operation)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + (operation.interval or 0)
        try:
            if operation.interval is None:
                operation.state = "running"
                await operation.callback(ctx)
                return
            first = operation.immediate
            while True:
                operation.state = "waiting"
                if not first:
                    try:
                        await asyncio.wait_for(
                            operation.wakeup.wait(),
                            timeout=max(0, deadline - loop.time()),
                        )
                    except TimeoutError:
                        pass
                first = False
                operation.wakeup.clear()
                if operation.authority is Authority.LEADER:
                    await self.worker.is_leader.wait()
                operation.state = "running"
                try:
                    await operation.callback(ctx)
                except LeadershipLost:
                    continue
                if loop.time() >= deadline:
                    deadline = loop.time() + operation.interval
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            operation.error = exc
            operation.state = "failed"
            raise
        finally:
            if operation.state != "failed":
                operation.state = "stopped"

    async def close(self) -> None:
        """
        Cancel tasks and release resources. Called by the worker.

        Concurrent callers share the same cleanup. Cancelling a caller leaves
        cleanup running; a later call waits for its result.
        """
        if self._close_task is None:
            self._closed = True
            self._close_task = asyncio.create_task(self._close())
        await asyncio.shield(self._close_task)

    async def _close(self) -> None:
        try:
            for task in self._tasks:
                task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
        finally:
            await self._resources.aclose()
