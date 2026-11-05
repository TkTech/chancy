import asyncio
import dataclasses
import datetime
import inspect
import json
import math
import platform
import random
import re
import signal
import socket
import sys
import time
import uuid
import warnings
from collections import defaultdict, deque
from collections.abc import Coroutine, Iterable
from contextlib import AsyncExitStack, suppress

from psycopg import AsyncConnection, InterfaceError, OperationalError, sql
from psycopg.errors import LockNotAvailable
from psycopg.pq import TransactionStatus
from psycopg.rows import dict_row
from psycopg.types.json import Json

from chancy.app import Chancy
from chancy.errors import MigrationsNeededError, UnconfirmedClaims
from chancy.executors.base import Executor
from chancy.hub import Event, Hub
from chancy.job import QueuedJob, Reference
from chancy.leadership import LeadershipLease
from chancy.plugin import PluginScope
from chancy.queue import Queue
from chancy.retry import DatabaseRetry, _raise_if_cancelling
from chancy.session import PluginSession
from chancy.utils import (
    ExceptionSnapshot,
    TaskManager,
    import_string,
    lock_order_key,
    sleep,
)


class Worker:
    """
    The Worker is responsible for polling queues for new jobs, running any
    configured plugins, and internal management such as heartbeats.

    Starting a worker is simple:

    .. code-block:: python

        async with Chancy(
            "postgresql://localhost/postgres",
        ) as chancy:
            await chancy.migrate()
            async with Worker(chancy) as worker:
                await worker.wait_for_shutdown()

    Worker Tags
    -----------

    Worker tags control which workers run which queues. A tag is a string
    (typically in the format ``key=value``) assigned to a worker. Declare
    a queue with a list of tags, and only workers with matching tags will
    run that queue.

    Every worker automatically gets some tags, like ``hostname``, ``worker_id``,
    ``python``, ``arch``, ``os``, and ``*`` (wildcard). You can also add custom
    tags when creating a worker.

    .. code-block:: python

        async with Worker(chancy, tags={"has=gpu", "has=large-disk"}) as worker:
            await worker.wait_for_shutdown()

    You could then assign a queue to only run on workers with the ``has=gpu`` tag:

    .. code-block:: python

        await chancy.declare(
            Queue(name="default", tags={"has=gpu"}, concurrency=10),
            upsert=True  # Replaces existing settings
        )

    Tags are regexes, allowing for flexible matching:

    .. code-block:: python

        Queue(name="default", tags={r"python=3\\.11\\.[0-9]+"}, concurrency=10)

    Events
    ------

    The worker emits events that can be listened to by plugins. The following
    events are emitted:

    .. list-table::
        :header-rows: 1

        * - Event
          - Kwargs
          - Description
        * - worker.started
          - worker
          - Emitted when the worker is started.
        * - worker.stopped
          - worker
          - Emitted when the worker is stopped.
        * - worker.shutdown_timeout
          - worker
          - Emitted when cleanup finishes after an earlier shutdown attempt
            timed out while draining jobs.
        * - worker.queue.started
          - queue, executor, worker
          - Emitted when a queue is started.
        * - worker.queue.removed
          - queue, executor, worker
          - Emitted when a queue is removed.
        * - worker.queue.updated
          - queue, executor, worker
          - Emitted when a queue is updated.

    :param chancy: The Chancy application.
    :param worker_id: The ID of the worker, which must be globally unique. If
                      not provided, a random UUID will be generated.
    :param heartbeat_poll_interval: The number of seconds between heartbeat
                                    poll intervals.
    :param heartbeat_timeout: The number of seconds before a worker is
                              considered to have lost connection to the
                              cluster.
    :param queue_change_poll_interval: The number of seconds between checks for
                                       changes to the queues that the worker
                                       should be processing.
    :param shutdown_timeout: Maximum seconds for the entire shutdown, including
                             draining, task cancellation, and final persistence.
    :param leadership_eligible: Participate in cluster leadership election
                                (default True). Set False to exclude this worker.
    :param leadership_poll_interval: Seconds between lease renewal/election
                                     attempts (default 60). The first attempt
                                     is immediate.
    :param leadership_timeout: Lease lifetime in seconds (default 120). Must be
                               greater than the poll interval; twice it is
                               recommended.
    :param leadership_idle_timeout: Maximum idle time between SQL commands in
                                    a protected transaction, in seconds (default
                                    120). Active SQL is unaffected. Stricter
                                    database settings are preserved; None leaves
                                    the database setting unchanged.
    :param tags: Extra tags to associate with the worker.
    :param register_signal_handlers: Whether to register signal handlers.
    :param backoff_initial: The initial ceiling in seconds for retry
                            delays after a transient database error in
                            internal maintenance loops. Each actual delay
                            is drawn uniformly from ``[0, ceiling]`` (full
                            jitter), so workers don't reconnect in
                            lockstep.
    :param backoff_max: The maximum ceiling in seconds. Exponential
                        growth of the ceiling is capped at this value.
    :param backoff_max_retries: The maximum number of consecutive retries
                                before allowing the error to propagate.
                                If ``None``, retries indefinitely.
    """

    def __init__(
        self,
        chancy: Chancy,
        *,
        worker_id: str | None = None,
        heartbeat_poll_interval: int = 30,
        heartbeat_timeout: int = 90,
        queue_change_poll_interval: int = 10,
        shutdown_timeout: int = 30,
        leadership_eligible: bool = True,
        leadership_poll_interval: float = 60,
        leadership_timeout: float = 120,
        leadership_idle_timeout: int | None = 120,
        tags: set[str] | None = None,
        register_signal_handlers: bool = True,
        backoff_initial: float = 1.0,
        backoff_max: float = 60.0,
        backoff_max_retries: int | None = None,
    ):
        if not math.isfinite(shutdown_timeout) or shutdown_timeout < 0:
            raise ValueError("shutdown_timeout must be finite and non-negative")
        #: The Chancy application that the worker is associated with.
        self.chancy = chancy
        #: The ID of the worker, which must be globally unique.
        self.worker_id = worker_id or str(uuid.uuid4())
        #: The number of seconds between heartbeat poll intervals.
        self.heartbeat_poll_interval = heartbeat_poll_interval
        #: The number of seconds before a worker is considered to have lost
        #: connection to the cluster.
        self.heartbeat_timeout = heartbeat_timeout
        #: Maximum time for the entire shutdown attempt.
        self.shutdown_timeout = shutdown_timeout
        #: The number of seconds between sending outgoing updates to the
        #: database.
        self.send_outgoing_interval = 1
        #: How frequently to check for changes to the queues that this worker
        #: should be processing.
        self.queue_change_poll_interval = queue_change_poll_interval
        #: An event hub with cluster and local telemetry, useful for plugins.
        self.hub = Hub(log=chancy.log)
        #: Whether to register signal handlers on startup of the worker.
        self.register_signal_handlers = register_signal_handlers
        #: Initial backoff delay in seconds for transient errors in
        #: internal maintenance loops.
        self.backoff_initial = backoff_initial
        #: Maximum backoff delay in seconds for transient errors in
        #: internal maintenance loops.
        self.backoff_max = backoff_max
        #: Maximum consecutive retries before propagating the error,
        #: or ``None`` for unlimited retries.
        self.backoff_max_retries = backoff_max_retries
        # Extra tags to associate with the worker.
        self._extra_tags = tags or set()

        #: The task manager for the worker, responsible for managing all the
        #: worker's internal asyncio tasks.
        self.manager = TaskManager(
            log=chancy.log,
            on_failure=self._on_task_failure,
            on_exit=self._on_task_exit,
        )
        self._task_returns: dict[asyncio.Task, bool] = {}

        #: A queue of updates waiting to be sent to the database.
        self.outgoing: asyncio.Queue[QueuedJob] = asyncio.Queue()
        self._outgoing_lock = asyncio.Lock()
        # Keep an uncommitted batch ahead of new updates across flush attempts.
        self._pending_updates: list[QueuedJob] = []
        self._accepting_updates = True

        self._leadership = LeadershipLease(
            self,
            eligible=leadership_eligible,
            poll_interval=leadership_poll_interval,
            timeout=leadership_timeout,
            idle_timeout=leadership_idle_timeout,
        )
        self._plugin_sessions: list[PluginSession] = []
        #: An event that is set when the worker is shutting down.
        self.shutdown_event = asyncio.Event()
        self._signal_received = False
        #: Events set whenever a job is pushed to a queue.
        self.queue_wake_events: dict[str, asyncio.Event] = defaultdict(
            asyncio.Event
        )

        # Set once the notifications listener is ready.
        self._notifications_ready_event = asyncio.Event()
        # The queues that the worker is currently processing.
        self._queues: dict[str, Queue] = {}
        # The executors that the worker is currently using.
        self._executors: dict[str, Executor] = {}
        # Shared by signal handlers, context manager cleanup, and callers.
        self._stop_task: asyncio.Task[bool] | None = None
        self._cleanup_task: asyncio.Task | None = None
        # Preserve the teardown result if final persistence needs a retry.
        self._drain_result: bool | None = None
        self._starting = False
        self._failure_event = asyncio.Event()
        self._stop_requested = asyncio.Event()
        self._finalized = False
        self._cleanup_errors: list[ExceptionSnapshot] = []
        self._stop_errors: dict[asyncio.Task, ExceptionSnapshot] = {}
        self._stopped_sessions: set[PluginSession] = set()
        self._queues_task: asyncio.Task | None = None
        self._queue_tasks: dict[str, asyncio.Task] = {}

    @property
    def healthy(self) -> bool:
        """Whether the worker is accepting work without terminal failures."""
        return not (self.shutdown_event.is_set() or self.manager.errors)

    def _on_task_failure(self) -> None:
        self._failure_event.set()
        if not self._starting and self._stop_task is None:
            self._begin_stop(self.shutdown_timeout)

    @property
    def is_leader(self) -> bool:
        """Whether this worker currently has a locally valid leadership lease.

        This observation cannot authorize database writes: leader operations
        verify and lock the lease inside each transaction.
        """
        return self._leadership.is_leader

    async def wait_for_leadership(self) -> None:
        """Wait for a valid lease. Ineligible workers never acquire leadership."""
        await self._leadership.wait()

    def _start_task(
        self, name: str, coroutine: Coroutine, *, allow_return: bool = False
    ) -> asyncio.Task:
        """Supervise required work while allowing explicitly finite tasks."""
        task = self.manager.add(name, coroutine)
        self._task_returns[task] = allow_return
        return task

    def _on_task_exit(
        self, task: asyncio.Task, cancellation_requested: bool
    ) -> BaseException | None:
        allow_return = self._task_returns.pop(task, None)
        if allow_return is None or self.shutdown_event.is_set():
            return None
        if task.cancelled():
            if not cancellation_requested:
                return RuntimeError(
                    f"Task {task.get_name()!r} was unexpectedly cancelled"
                )
        elif task.exception() is None and not allow_return:
            return RuntimeError(f"Task {task.get_name()!r} exited unexpectedly")
        return None

    async def start(self):
        """
        Start the worker and begin processing jobs.

        This method will return immediately - to wait until the worker is
        stopped, use the `wait_for_shutdown` method:

        .. code-block:: python

            worker = Worker(chancy)
            await worker.start()
            await worker.wait_for_shutdown()

        If possible, it's better to use the worker as an async context manager
        to ensure that it's properly cleaned up:

        .. code-block:: python

            async with Worker(chancy) as worker:
                await worker.wait_for_shutdown()
        """
        self._starting = True
        try:
            await self._start()
        except BaseException as error:  # noqa: BLE001 - re-raised after cleanup
            await self._abort_start(error)
        finally:
            self._starting = False

    async def _abort_start(self, error: BaseException) -> None:
        """Release partial setup without running final persistence hooks."""
        self.shutdown_event.set()
        errors = [ExceptionSnapshot(error)]
        cancellation = (
            error if isinstance(error, asyncio.CancelledError) else None
        )
        try:
            task = asyncio.create_task(self._abort_cleanup())
            self._cleanup_task = task
            task.add_done_callback(self._cleanup_done)
            await self._await_cleanup(
                task,
                asyncio.get_running_loop().time() + self.shutdown_timeout,
            )
            task.result()
        except BaseException as cleanup_error:
            self.chancy.log.error(
                "Startup teardown failed", exc_info=cleanup_error
            )
            errors.append(ExceptionSnapshot(cleanup_error))
            if isinstance(cleanup_error, asyncio.CancelledError):
                cancellation = cancellation or cleanup_error
        self._leadership.clear()
        if cancellation is not None:
            try:
                self.manager.raise_errors(
                    "Worker startup cleanup failed",
                    [
                        entry
                        for entry in (*errors, *self._cleanup_errors)
                        if entry.exception is not cancellation
                    ],
                )
            except BaseException as cleanup_error:
                raise cancellation from cleanup_error
            raise cancellation
        self.manager.raise_errors(
            "Worker startup failed", (*errors, *self._cleanup_errors)
        )

    async def _abort_cleanup(self) -> None:
        await self.manager.cancel_all()
        await self._close_sessions()

    async def _close_sessions(self) -> None:
        for session in reversed(self._plugin_sessions):
            try:
                await session.close()
            except BaseException as error:
                if isinstance(error, asyncio.CancelledError):
                    task = asyncio.current_task()
                    if task is not None and task.cancelling():
                        raise
                self._cleanup_errors.append(ExceptionSnapshot(error))
                self.chancy.log.error(
                    "Plugin session cleanup failed", exc_info=error
                )

    async def _start(self):
        for plugin in self.chancy.plugins.values():
            for name in (
                "run",
                "on_worker_started",
                "sleep",
                "wait_for_leader",
                "wake_up",
            ):
                if hasattr(plugin, name):
                    raise TypeError(
                        f"{type(plugin).__name__}.{name}() is no longer supported. "
                        "Use setup(session) and session operations; "
                        "cleanup hooks must accept (chancy, cursor)."
                    )
            try:
                inspect.signature(plugin.cleanup).bind(self.chancy, None)
            except TypeError as exc:
                raise TypeError(
                    f"{type(plugin).__name__}.cleanup() must accept (chancy, cursor)."
                ) from exc

        if not await self.chancy.is_up_to_date():
            raise MigrationsNeededError()

        self.hub.on("job.cancelled", self._handle_cancellation)
        self.hub.on("job.recovered", self._handle_recovery)
        self.hub.on("queue.pushed", self._handle_queue_pushed)

        for plugin in self.chancy.plugins.values():
            if plugin.get_scope() != PluginScope.WORKER:
                continue

            if set(plugin.get_dependencies()) - self.chancy.plugins.keys():
                raise ValueError(
                    f"Plugin {plugin.get_identifier()!r} has unsatisfied "
                    f"dependencies: {plugin.get_dependencies()}"
                )

            session = PluginSession(plugin, self)
            self._plugin_sessions.append(session)
            await plugin.setup(session)

        if self._leadership.eligible:
            self._start_task("leadership", self._leadership.maintain())
            self._start_task("leadership_expiry", self._leadership.expire())

        for session in self._plugin_sessions:
            session.start()

        self._queues_task = self._start_task("queues", self._maintain_queues())
        self._start_task("updates", self._maintain_updates())
        self._start_task("heartbeat", self._maintain_heartbeat())

        # pgbouncer and other connection pools may not support LISTEN/NOTIFY
        # properly, so this needs to remain an optional (but default) feature.
        if self.chancy.notifications:
            self._start_task("notifications", self._maintain_notifications())
            ready = asyncio.create_task(self._notifications_ready_event.wait())
            failed = asyncio.create_task(self._failure_event.wait())
            try:
                await asyncio.wait(
                    {ready, failed}, return_when=asyncio.FIRST_COMPLETED
                )
                errors = self.manager.errors
                if errors:
                    raise errors[0]
            finally:
                ready.cancel()
                failed.cancel()
                await asyncio.gather(ready, failed, return_exceptions=True)

        if self.register_signal_handlers and sys.platform == "win32":
            warnings.warn(
                "Signal handlers are not supported on Windows, "
                "use CTRL+C to stop the worker.",
                RuntimeWarning,
            )
        elif self.register_signal_handlers:
            loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
            for sig in {signal.SIGTERM, signal.SIGINT}:
                loop.add_signal_handler(
                    sig,
                    lambda s=sig: asyncio.create_task(self.on_signal(s)),
                )

        await self.chancy.declare(Queue(name="default"))
        await self.hub.emit("worker.started", {"worker": self})
        errors = self.manager.errors
        if errors:
            raise errors[0]

    async def wait_for_shutdown(self):
        """
        Wait for teardown, then report the worker's outcome to every caller.

        Supervision runs independently of this method. One failure is raised
        directly; multiple failures are reported together in an exception group.
        """
        await self._stop_requested.wait()
        return await self._wait_for_stop()

    def _calculate_backoff(self, consecutive_failures: int) -> float:
        """Calculate capped exponential backoff delay with full jitter.

        Applies the "full jitter" strategy: the delay is a uniform random
        value between 0 and the capped exponential ceiling. This prevents
        multiple workers from reconnecting in lockstep after a shared
        outage.

        :param consecutive_failures: The number of consecutive failures so
                                     far (0-indexed).
        :returns: The delay in seconds before the next retry.
        """
        ceiling = self.backoff_initial
        for _ in range(consecutive_failures):
            if ceiling == 0 or ceiling >= self.backoff_max:
                break
            ceiling *= 2
        return random.uniform(0, min(ceiling, self.backoff_max))

    def database_retry(self, name: str) -> DatabaseRetry:
        """Create independent retry state using this worker's backoff settings."""
        return DatabaseRetry(
            name,
            calculate_delay=self._calculate_backoff,
            max_retries=self.backoff_max_retries,
            log=self.chancy.log,
        )

    async def _maintain_queues(self):
        """
        Maintain the queues that the worker is processing.

        This will periodically check the database for any changes to the queues
        that the worker should be processing, and update the worker's queues
        accordingly.
        """
        retry = self.database_retry("queue maintenance")
        while True:
            try:
                self.chancy.log.debug("Polling for queue changes.")
                tags = self.worker_tags()
                # Tags in the database is a list of regexes, while the
                # tags in the worker are a set of strings. We need to
                # filter the queues based on the worker's tags.
                db_queues = {
                    q.name: q
                    for q in await self.chancy.get_all_queues()
                    if any(re.match(t, tag) for tag in tags for t in q.tags)
                }

                for queue_name in list(self._queues.keys()):
                    if queue_name not in db_queues:
                        del self._queues[queue_name]
                        self.queue_wake_events.pop(queue_name, None)
                        self.chancy.log.info(f"Removed queue {queue_name}")

                for queue_name, queue in db_queues.items():
                    if queue_name not in self._queues:
                        self._queues[queue_name] = queue
                        self.chancy.log.info(
                            f"Adding queue {queue_name!r} to worker"
                            f" using executor {queue.executor!r}."
                        )
                    else:
                        if self._queues[queue_name] != queue:
                            self._queues[queue_name] = queue
                            self.chancy.log.info(
                                f"Updated configuration for the"
                                f" queue {queue_name!r}."
                            )

                    # A removed queue may be re-added while its previous loop
                    # drains. Let that loop adopt the new configuration first.
                    task = self._queue_tasks.get(queue_name)
                    if task is None or task.done():
                        task = self._start_task(
                            f"queue_{queue_name}",
                            self._maintain_queue(queue),
                            allow_return=True,
                        )
                        self._queue_tasks[queue_name] = task
                        task.add_done_callback(
                            lambda done, name=queue_name: self._queue_finished(
                                name, done
                            )
                        )

                retry.reset()
            except retry.errors as exc:
                await retry.wait(exc)
                continue

            try:
                await self.hub.wait_for(
                    ["queue.declared", "queue.paused", "queue.resumed"],
                    timeout=self.queue_change_poll_interval,
                )
            except TimeoutError:
                pass

    def _queue_finished(self, name: str, task: asyncio.Task) -> None:
        if self._queue_tasks.get(name) is task:
            del self._queue_tasks[name]

    async def _maintain_queue(self, queue: Queue):
        """
        Maintain a single queue.

        Responsible for polling a queue for new jobs and pushing them to the
        executor.

        :param queue: The queue to maintain.
        """
        retry = self.database_retry(f"queue {queue.name!r}")
        pending: deque[QueuedJob] = deque()
        while True:
            current_queue = self._queues.get(queue.name)
            if current_queue is None:
                return
            queue = current_queue
            cls = import_string(queue.executor)

            async with AsyncExitStack() as stack:
                stack.callback(self._executors.pop, queue.name, None)
                async with cls(
                    self, queue, **queue.executor_options
                ) as executor:
                    self._executors[queue.name] = executor

                    concurrency = queue.concurrency
                    if concurrency is None:
                        concurrency = executor.get_default_concurrency()

                    await self.hub.emit(
                        "worker.queue.started",
                        {
                            "queue": queue,
                            "executor": executor,
                            "worker": self,
                        },
                    )

                    # Start with the event set so the loop runs
                    # immediately on the first iteration.
                    self.queue_wake_events[queue.name].set()

                    while await sleep(
                        queue.polling_interval,
                        events=[self.queue_wake_events[queue.name].wait()],
                    ):
                        try:
                            self.queue_wake_events[queue.name].clear()
                            if pending:
                                resolved = await self._resolve_claims(pending)
                                if resolved is None:
                                    retry.reset()
                                    continue
                                pending = deque(resolved)
                            else:
                                new_queue = self._queues.get(queue.name)

                                # There's been a change in the state of the
                                # queue, we'll let the executor drain before
                                # we do anything else.
                                if new_queue != queue and len(executor) > 0:
                                    continue
                                # The queue was completely removed and the
                                # executor has been drained, terminate.
                                elif new_queue is None:
                                    await self.hub.emit(
                                        "worker.queue.removed",
                                        {
                                            "queue": queue,
                                            "executor": executor,
                                            "worker": self,
                                        },
                                    )
                                    return
                                # The queue still exists but has been
                                # reconfigured, so we break out to the outer
                                # loop to start a new executor.
                                elif new_queue != queue and new_queue:
                                    await self.hub.emit(
                                        "worker.queue.updated",
                                        {
                                            "queue": queue,
                                            "executor": executor,
                                            "worker": self,
                                        },
                                    )
                                    break
                                # Do nothing at all if the queue is paused,
                                # unless it had an automatic resume time set.
                                elif queue.state == Queue.State.PAUSED:
                                    if (
                                        queue.resume_at is not None
                                        and queue.resume_at
                                        < datetime.datetime.now(
                                            tz=queue.resume_at.tzinfo
                                        )
                                    ):
                                        await self.chancy.resume_queue(
                                            queue.name
                                        )
                                        queue = dataclasses.replace(
                                            queue,
                                            state=Queue.State.ACTIVE,
                                        )
                                # Otherwise, we can fetch jobs from the queue
                                # until the executor is full.
                                else:
                                    maximum_jobs_to_poll = concurrency - len(
                                        executor
                                    )
                                    if maximum_jobs_to_poll <= 0:
                                        await self.hub.emit(
                                            "worker.queue.full",
                                            {
                                                "queue": queue,
                                                "executor": executor,
                                                "worker": self,
                                            },
                                        )
                                        continue

                                    async with (
                                        self.chancy.pool.connection() as conn
                                    ):
                                        pending.extend(
                                            await self.fetch_jobs(
                                                queue,
                                                conn,
                                                up_to=maximum_jobs_to_poll,
                                            )
                                        )

                            while pending:
                                job = pending[0]
                                self.chancy.log.info(
                                    f"Pulled {job.id!r}"
                                    f" ({job.func!r}) for"
                                    f" queue {job.queue!r}"
                                )
                                await executor.push(job)
                                pending.popleft()
                            retry.reset()
                        except retry.errors as exc:
                            if isinstance(exc, UnconfirmedClaims):
                                pending.extend(exc.jobs)
                            await retry.wait(exc)
                            self.queue_wake_events[queue.name].set()

    async def _maintain_heartbeat(self):
        """
        Announces the worker to the cluster, and maintains a periodic heartbeat
        to ensure that the worker is still alive.
        """
        retry = self.database_retry("heartbeat")
        while True:
            try:
                async with (
                    self.chancy.pool.connection() as conn,
                    conn.transaction(),
                ):
                    self.chancy.log.debug("Announcing worker to the cluster.")
                    await self.announce_worker(conn)
                retry.reset()
            except retry.errors as exc:
                await retry.wait(exc)
                continue
            await asyncio.sleep(self.heartbeat_poll_interval)

    async def _maintain_notifications(self):
        """
        Listen for notifications from the database.

        Improves the reactivity of a worker by allowing it to almost immediately
        react to cluster events using Postgres's LISTEN/NOTIFY feature.

        .. note::

            This feature utilizes a permanent connection to the database
            separate from the shared connection pool and is not counted against
            the pool's connection limit.
        """
        retry = self.database_retry("notifications listener")
        ever_connected = False
        while True:
            connection = None
            try:
                connection = await AsyncConnection.connect(
                    self.chancy.dsn, autocommit=True
                )
                await connection.execute(
                    sql.SQL("LISTEN {channel};").format(
                        channel=sql.Identifier(f"{self.chancy.prefix}events")
                    )
                )
                self.chancy.log.info(
                    "Started listening for realtime notifications."
                )
                self._notifications_ready_event.set()
                ever_connected = True
                retry.reset()
                async for notification in connection.notifies():
                    j = json.loads(notification.payload)
                    await self.hub.emit(j.pop("t"), j, isolate_errors=True)
            except retry.errors as exc:
                _raise_if_cancelling()
                if not ever_connected:
                    raise
                await retry.wait(exc)
            finally:
                if connection is not None:
                    try:
                        await connection.close()
                    except (OperationalError, InterfaceError):
                        pass

    async def _maintain_updates(self):
        """
        Process updates to job instances.

        We maintain a queue of updates to job instances, and process them in
        batches to significantly reduce the number of overall transactions that
        need to be made, at the cost of potentially losing some updates if the
        worker is stopped unexpectedly. The frequency of these updates can be
        controlled by setting the `send_outgoing_interval` attribute on the
        worker.
        """
        retry = self.database_retry("outgoing job updates")
        while True:
            if not self._pending_updates and self.outgoing.empty():
                await asyncio.sleep(self.send_outgoing_interval)
                continue

            try:
                await self.flush()
            except retry.errors as exc:
                await retry.wait(exc)
                continue

            retry.reset()
            await asyncio.sleep(self.send_outgoing_interval)

    async def flush(self) -> None:
        """
        Persist pending job updates without stopping the worker.

        Updates accepted by :meth:`queue_update` are written in batches until
        the outgoing queue is empty. This also waits for each batch's
        ``on_job_updated`` plugin hooks, but does not wait for running jobs to
        finish. Concurrent calls, including the periodic writer and shutdown,
        are serialized.

        Database errors propagate to the caller. If a write fails or is
        cancelled, its batch is retained ahead of newer updates for a later
        attempt. Earlier batches may already be committed.

        Within each batch, database writes follow unique-key order to avoid
        deadlocks with concurrent pushes. Updates to the same job retain their
        original order, as do the plugin callbacks.

        ``on_jobs_updated_in_transaction`` hooks run before commit using the
        update transaction; failures roll back the batch. ``on_job_updated``
        hooks run only after the transaction has committed.

        Updates from a claim invalidated by recovery, a new execution, or a
        manual retry are discarded without invoking either update hook.

        .. code-block:: python

            await worker.queue_update(updated_job)
            await worker.flush()
        """
        async with self._outgoing_lock:
            while self._pending_updates or not self.outgoing.empty():
                pending_updates = self._pending_updates
                while len(pending_updates) < 1000:
                    try:
                        pending_updates.append(self.outgoing.get_nowait())
                    except asyncio.QueueEmpty:
                        break

                self.chancy.log.debug(
                    f"Processing {len(pending_updates)} outgoing updates."
                )

                # Match push_many_ex's lock order. The stable sort preserves
                # updates to the same job; keep the retained batch and hooks
                # in their original order.
                ordered_updates = sorted(
                    enumerate(pending_updates),
                    key=lambda item: (
                        lock_order_key(item[1].unique_key),
                        item[1].id,
                    ),
                )

                async with (
                    self.chancy.pool.connection() as conn,
                    conn.cursor(row_factory=dict_row) as cursor,
                    conn.transaction(),
                ):
                    await cursor.executemany(
                        sql.SQL(
                            """
                            UPDATE
                                {jobs}
                            SET
                                state = %(state)s,
                                started_at = %(started_at)s,
                                completed_at = %(completed_at)s,
                                scheduled_at = %(scheduled_at)s,
                                attempts = %(attempts)s,
                                errors = %(errors)s,
                                meta = %(meta)s,
                                max_attempts = %(max_attempts)s
                            WHERE
                                id = %(id)s
                                AND claim_id IS NOT DISTINCT FROM %(claim_id)s
                            RETURNING id
                            """
                        ).format(
                            jobs=sql.Identifier(f"{self.chancy.prefix}jobs")
                        ),
                        [
                            {
                                "id": update.id,
                                "claim_id": update.claim_id,
                                "state": update.state.value,
                                "started_at": update.started_at,
                                "completed_at": update.completed_at,
                                "scheduled_at": update.scheduled_at,
                                "attempts": update.attempts,
                                "errors": Json(update.errors),
                                "meta": Json(update.meta),
                                "max_attempts": update.max_attempts,
                            }
                            for _, update in ordered_updates
                        ],
                        returning=True,
                    )

                    accepted = set()
                    for position, _ in ordered_updates:
                        if await cursor.fetchone() is not None:
                            accepted.add(position)
                        cursor.nextset()

                    updates = tuple(
                        update
                        for position, update in enumerate(pending_updates)
                        if position in accepted
                    )
                    if updates:
                        for plugin in self.chancy.plugins.values():
                            await plugin.on_jobs_updated_in_transaction(
                                worker=self, jobs=updates, cursor=cursor
                            )

                # Only discard the batch once its transaction has committed.
                self._pending_updates = []

                for update in updates:
                    for plugin in self.chancy.plugins.values():
                        await plugin.on_job_updated(job=update, worker=self)

    async def announce_worker(self, conn: AsyncConnection):
        """
        Announce the worker to the cluster.

        This will insert the worker into the workers table, or update the
        `last_seen` timestamp if the worker is already present.

        :param conn: The connection to use for the announcement.
        """
        async with conn.cursor(row_factory=dict_row) as cur, conn.transaction():
            await cur.execute(
                sql.SQL(
                    """
                        DELETE FROM {workers}
                        WHERE expires_at < NOW()
                        """
                ).format(workers=sql.Identifier(f"{self.chancy.prefix}workers"))
            )
            await cur.execute(
                sql.SQL(
                    """
                        INSERT INTO {workers}
                            (worker_id, last_seen, expires_at, tags, queues)
                        VALUES (
                            %(worker_id)s,
                            NOW(),
                            NOW() + INTERVAL {timeout},
                            %(tags)s,
                            %(queues)s
                        )
                        ON CONFLICT (worker_id) DO UPDATE
                            SET last_seen = NOW(),
                                expires_at = NOW() + INTERVAL {timeout},
                                tags = EXCLUDED.tags,
                                queues = EXCLUDED.queues
                        """
                ).format(
                    workers=sql.Identifier(f"{self.chancy.prefix}workers"),
                    timeout=sql.Literal(f"{self.heartbeat_timeout} seconds"),
                ),
                {
                    "worker_id": self.worker_id,
                    "tags": list(self.worker_tags()),
                    "queues": list(self._queues.keys()),
                },
            )
            await self.chancy.notify(
                cur, "worker.announced", {"worker_id": self.worker_id}
            )

    def worker_tags(self) -> set[str]:
        """
        Return the tags associated with the worker.

        Tags are used to limit which Queues get assigned to a worker, and
        may also be used by some plugins.

        :return: A set of tags associated with the worker.
        """
        return {
            "*",
            f"hostname={socket.gethostname()}",
            f"worker_id={self.worker_id}",
            f"python={platform.python_version()}",
            f"os={platform.system()}",
            f"arch={platform.machine()}",
            *self._extra_tags,
        }

    async def queue_update(self, update: QueuedJob) -> bool:
        """
        Enqueue an update to a job instance.

        This method will queue an update to a job instance to be processed in
        periodic batches, reducing the number of transactions that need to be
        made. You can control the frequency of these updates by setting the
        `send_outgoing_interval` attribute on the worker. Await :meth:`flush`
        after an accepted update to persist it immediately.

        :param update: The job instance to update.
        :return: Whether the update was accepted by the worker.
        """
        if not self._accepting_updates:
            return False

        self.outgoing.put_nowait(update)
        return True

    async def _resolve_claims(
        self, jobs: Iterable[QueuedJob]
    ) -> list[QueuedJob] | None:
        """Return current claims, or None while a row is locked."""
        jobs = list(jobs)
        async with self.chancy.pool.connection() as conn:
            try:
                async with (
                    conn.transaction(),
                    conn.cursor(row_factory=dict_row) as cursor,
                ):
                    # Lock by job ID before checking the claim: an unfinished
                    # transaction may still expose the previous claim to readers.
                    await cursor.execute(
                        sql.SQL(
                            "SELECT * FROM {} WHERE id = ANY(%s) "
                            "ORDER BY id FOR UPDATE NOWAIT"
                        ).format(sql.Identifier(f"{self.chancy.prefix}jobs")),
                        [[job.id for job in jobs]],
                    )
                    rows = {row["id"]: row for row in await cursor.fetchall()}
            except LockNotAvailable:
                _raise_if_cancelling()
                # Psycopg may preserve the lock error after a failed rollback.
                if (
                    conn.closed
                    or conn.info.transaction_status != TransactionStatus.IDLE
                ):
                    raise
                return None
        resolved = []
        for job in jobs:
            row = rows.get(job.id)
            if (
                row is not None
                and row["state"] == QueuedJob.State.RUNNING.value
                and row["taken_by"] == self.worker_id
                and row["claim_id"] == job.claim_id
            ):
                resolved.append(QueuedJob.unpack(row))
        return resolved

    async def fetch_jobs(
        self,
        queue: Queue,
        conn: AsyncConnection,
        *,
        up_to: int = 1,
    ) -> list[QueuedJob]:
        """
        Fetch jobs from the queue for processing.

        This method will fetch up to `up_to` jobs from the queue, mark them as
        running, and return them as a list of `QueuedJob` objects. If no jobs
        are available, an empty list will be returned.

        It's safe to call this method concurrently, as the jobs will be locked
        for the duration of the transaction.

        An uncertain commit raises :class:`~chancy.errors.UnconfirmedClaims`
        with the affected jobs. The worker resolves these before fetching more.
        When called within an existing transaction, the caller owns its commit.

        :param queue: The queue to fetch jobs from.
        :param conn: The database connection to use.
        :param up_to: The maximum number of jobs to fetch.
        """
        jobs_table = sql.Identifier(f"{self.chancy.prefix}jobs")
        rate_limits_table = sql.Identifier(
            f"{self.chancy.prefix}queue_rate_limits"
        )

        owned_commit = conn.info.transaction_status == TransactionStatus.IDLE
        jobs: list[QueuedJob] = []
        try:
            async with (
                conn.cursor(row_factory=dict_row) as cursor,
                conn.transaction(),
            ):
                # If the queue is configured to use a rate limit, we need to
                # check if there's any remaining capacity in the current
                # window.
                if queue.rate_limit:
                    assert queue.rate_limit_window is not None
                    now = int(time.time())
                    window_start = now - (now % queue.rate_limit_window)

                    await cursor.execute(
                        sql.SQL(
                            """
                                INSERT INTO {rate_limits_table} (
                                    queue,
                                    window_start,
                                    count
                                )
                                VALUES (%s, %s, 0)
                                ON CONFLICT (queue) DO UPDATE
                                SET
                                    count = CASE
                                        WHEN {rate_limits_table}.window_start
                                            = EXCLUDED.window_start
                                                THEN {rate_limits_table}.count
                                        ELSE 0
                                    END,
                                    window_start = EXCLUDED.window_start
                                RETURNING count
                                """
                        ).format(rate_limits_table=rate_limits_table),
                        (queue.name, window_start),
                    )

                    result = await cursor.fetchone()
                    assert result is not None
                    current_count = result["count"]

                    # If we've hit the rate limit, return early with no jobs
                    # fetched.
                    if current_count >= queue.rate_limit:
                        return []

                    # Adjust up_to based on remaining rate limit
                    up_to = min(up_to, queue.rate_limit - current_count)

                await cursor.execute(
                    sql.SQL(
                        """
                            WITH selected_jobs AS (
                                SELECT
                                    id
                                FROM
                                    {jobs}
                                WHERE
                                    queue = %(queue)s
                                AND
                                    (state = 'pending' OR state = 'retrying')
                                AND
                                    attempts < max_attempts
                                AND
                                    (scheduled_at IS NULL OR scheduled_at <= NOW())
                                ORDER BY
                                    priority DESC,
                                    id ASC
                                LIMIT
                                    %(maximum_jobs_to_fetch)s
                                FOR UPDATE OF {jobs} SKIP LOCKED
                            )
                            UPDATE
                                {jobs}
                            SET
                                started_at = NOW(),
                                state = 'running',
                                claim_id = gen_random_uuid(),
                                taken_by = %(worker_id)s
                            FROM
                                selected_jobs
                            WHERE
                                {jobs}.id = selected_jobs.id
                            RETURNING {jobs}.*
                            """
                    ).format(
                        jobs=jobs_table,
                    ),
                    {
                        "queue": queue.name,
                        "maximum_jobs_to_fetch": up_to,
                        "worker_id": self.worker_id,
                    },
                )

                records = await cursor.fetchall()

                # If a rate limit is configured, and we ended up fetching jobs,
                # we need to increment the rate limit counter.
                if queue.rate_limit and records:
                    await cursor.execute(
                        sql.SQL(
                            """
                                UPDATE {rate_limits_table}
                                SET count = count + %s
                                WHERE queue = %s
                                """
                        ).format(rate_limits_table=rate_limits_table),
                        (len(records), queue.name),
                    )

                jobs = [QueuedJob.unpack(record) for record in records]
        except DatabaseRetry.errors as exc:
            if jobs and owned_commit:
                raise UnconfirmedClaims(jobs) from exc
            raise
        return jobs

    async def on_signal(self, signum: int):
        """
        Called when a signal is received.

        By default, handles SIGTERM and SIGINT by starting worker shutdown. If
        the signal is received again, it will force a shutdown.
        """
        if signum in (signal.SIGTERM, signal.SIGINT):
            if self._signal_received:
                self.chancy.log.warning(
                    "Received signal again, forcing shutdown."
                )
                self._force_shutdown()
                return

            self._signal_received = True
            # Failures are already logged and remain available to waiters.
            with suppress(Exception, BaseExceptionGroup):
                await self.stop()

    def _force_shutdown(self):
        asyncio.get_running_loop().stop()

    async def on_job_completed(self, *, queue: Queue, job: QueuedJob):
        """
        Called when a job is completed by an executor.

        If the queue is configured for eager polling, this will set the queue's
        wake event to immediately poll for more jobs.
        """
        if queue.eager_polling and queue.name in self._queues:
            self.queue_wake_events[queue.name].set()

    async def stop(self, *, timeout: float | None = None) -> bool:
        """
        Stop the worker.

        One deadline covers draining jobs, cancelling background tasks, saving
        pending updates, and running final hooks. Expiry requests cancellation
        and raises ``TimeoutError`` without waiting for cancellation to finish.
        Multiple failures are reported together in an exception group.

        A later call can finish incomplete shutdown with a new deadline. It
        does not overlap cleanup still running from an earlier attempt or repeat
        completed final hooks. Cancelling a caller does not cancel shutdown.

        Returns True when shutdown completes successfully, including after a
        retry. Jobs abandoned at the deadline are left for recovery, and their
        later updates are ignored.

        :param timeout: Maximum seconds for the entire shutdown attempt.
            Defaults to ``shutdown_timeout``. Concurrent callers share the
            deadline established by the first caller.
        """
        timeout = timeout if timeout is not None else self.shutdown_timeout
        if not math.isfinite(timeout) or timeout < 0:
            raise ValueError("timeout must be finite and non-negative")
        if (
            not self._finalized
            and self._stop_task is not None
            and self._stop_task.done()
            and (
                self._stop_task.cancelled()
                or self._stop_task.exception() is not None
            )
        ):
            self._stop_task = None

        if self._stop_task is None:
            self._begin_stop(timeout)

        return await self._wait_for_stop()

    def _begin_stop(self, timeout: float) -> None:
        self.shutdown_event.set()
        self._leadership.clear()
        deadline = asyncio.get_running_loop().time() + timeout
        self._stop_task = asyncio.create_task(self._stop(deadline))
        self._stop_task.add_done_callback(self._stop_done)
        self._stop_requested.set()

    def _stop_done(self, task: asyncio.Task) -> None:
        # Automatic shutdown has no caller to retrieve/log persistence errors.
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            self._stop_errors.setdefault(task, ExceptionSnapshot(error))
            if isinstance(error, TimeoutError):
                self.chancy.log.error("Worker shutdown failed: %s", error)
            else:
                self.chancy.log.error("Worker shutdown failed", exc_info=error)

    async def _wait_for_stop(self) -> bool:
        task = self._stop_task
        assert task is not None
        errors = []
        result = False
        try:
            result = await asyncio.shield(task)
        except (Exception, BaseExceptionGroup) as error:  # noqa: BLE001 - aggregated below
            errors.append(
                self._stop_errors.setdefault(task, ExceptionSnapshot(error))
            )
        self.manager.raise_errors(
            "Worker failed", (*self._cleanup_errors, *errors)
        )
        return result

    def _cleanup_done(self, task: asyncio.Task) -> None:
        if task.cancelled():
            return
        error = task.exception()
        if error is not None and task.cancelling():
            self.chancy.log.error("Shutdown cleanup failed", exc_info=error)

    async def _await_cleanup(self, task: asyncio.Task, deadline: float) -> None:
        remaining = deadline - asyncio.get_running_loop().time()
        try:
            if remaining > 0:
                done, _ = await asyncio.wait({task}, timeout=remaining)
                if done:
                    return
        except asyncio.CancelledError:
            if not task.cancelling():
                task.cancel()
            raise

        self._accepting_updates = False
        self.manager.request_cancel_all()
        if not task.cancelling():
            task.cancel()
        raise TimeoutError("Worker shutdown timed out")

    async def _stop(self, deadline: float) -> bool:
        try:
            if self._cleanup_task is not None and not self._cleanup_task.done():
                await self._await_cleanup(self._cleanup_task, deadline)

            task = asyncio.create_task(self._finish_stop())
            self._cleanup_task = task
            task.add_done_callback(self._cleanup_done)
            await self._await_cleanup(task, deadline)
            task.result()
        except TimeoutError:
            if self._drain_result is None:
                self._drain_result = False
            raise
        return True

    async def _finish_stop(self) -> None:
        if self._drain_result is None:
            self._drain_result = False
            drained = await self._drain()
            _raise_if_cancelling()
            self._drain_result = drained

        await self.manager.cancel_all()
        await self.flush()
        _raise_if_cancelling()
        for session in self._plugin_sessions:
            plugin = session.plugin
            if session not in self._stopped_sessions:
                await plugin.on_worker_stopped(worker=self)
                self._stopped_sessions.add(session)
                _raise_if_cancelling()
        await self._close_sessions()
        _raise_if_cancelling()
        self._leadership.clear()
        await self.hub.emit(
            "worker.stopped"
            if self._drain_result
            else "worker.shutdown_timeout",
            {"worker": self},
        )
        _raise_if_cancelling()
        self._finalized = True

    async def _drain(self) -> bool:
        """Stop fetching work and persist completions while executors drain."""
        if self._queues_task is not None:
            await self.manager.cancel(self._queues_task)
        self._queues.clear()
        for wake_event in self.queue_wake_events.values():
            wake_event.set()
        while self._executors:
            await asyncio.sleep(0.01)
        self._accepting_updates = False
        return True

    async def _handle_cancellation(self, event: Event):
        self.chancy.log.info(
            f"Received cancellation request for job {event.body['j']!r}"
        )
        for executor in self._executors.values():
            await executor.cancel(Reference(event.body["j"]))

    async def _handle_recovery(self, event: Event):
        """Stop only the abandoned execution, never its replacement."""
        job_id = uuid.UUID(event.body["j"])
        claim_id = uuid.UUID(event.body["c"])
        for queue_name, executor in list(self._executors.items()):
            try:
                for job in executor.get_running_jobs():
                    if job.id == job_id and job.claim_id == claim_id:
                        await executor.cancel_execution(job)
            except Exception:
                # Cancellation is best-effort; the invalidated claim already
                # prevents this execution from updating the recovered job.
                self.chancy.log.exception(
                    "Failed to cancel recovered job %s (claim %s) in queue %s.",
                    job_id,
                    claim_id,
                    queue_name,
                )

    async def _handle_queue_pushed(self, event: Event):
        q = event.body["q"]
        if q not in self._queues:
            return
        self.queue_wake_events[q].set()

    @property
    def executors(self) -> dict[str, Executor]:
        """
        Get the executors that the worker is currently using to service queues.

        :return: A dictionary of executors keyed by queue name.
        """
        return self._executors

    async def increment_counter(
        self, metric_key: str, value: float, *, unit: str = "count"
    ):
        """
        Increment a counter metric by the specified value.

        This method emits a metrics.counter event that metrics plugins can
        subscribe to. If no metrics plugin is active, the event is simply
        ignored.

        :param metric_key: The hierarchical key for the metric (e.g.,
            "workflow:created", "queue:default:throughput")
        :param unit: Unit shared by all observations of this metric key.
        :param value: The value to increment the counter by
        """
        await self.hub.emit(
            "metrics.counter", {"key": metric_key, "value": value, "unit": unit}
        )

    async def record_gauge(
        self, metric_key: str, value: float, *, unit: str = "number"
    ):
        """
        Record a gauge metric value.

        This method emits a metrics.gauge event that metrics plugins can
        subscribe to. If no metrics plugin is active, the event is simply
        ignored.

        :param metric_key: The hierarchical key for the metric (e.g.,
            "workflow:active_count", "queue:default:size")
        :param unit: Unit shared by all observations of this metric key.
        :param value: The current gauge value
        """
        await self.hub.emit(
            "metrics.gauge", {"key": metric_key, "value": value, "unit": unit}
        )

    async def record_histogram_value(
        self, metric_key: str, value: float, *, unit: str = "number"
    ):
        """
        Record a value for a histogram metric.

        This method emits a metrics.histogram event that metrics plugins can
        subscribe to. If no metrics plugin is active, the event is simply
        ignored.

        :param metric_key: The hierarchical key for the metric (e.g.,
            "workflow:execution_time", "job:my_function:duration")
        :param unit: Unit shared by all observations of this metric key.
        :param value: The value to record in the histogram
        """
        await self.hub.emit(
            "metrics.histogram",
            {"key": metric_key, "value": value, "unit": unit},
        )

    def __repr__(self):
        return f"<Worker({self.worker_id!r})>"

    def __iter__(self):
        return iter(self._queues.values())

    def __getitem__(self, key):
        return self._queues[key]

    def __contains__(self, key):
        return key in self._queues

    def __len__(self):
        return len(self._queues)

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *exc):
        if self._stop_task is None:
            await self.stop()
        else:
            await self._wait_for_stop()
