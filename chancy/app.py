import asyncio
import enum
import functools
import logging
from typing import Any, Iterator
from functools import cached_property, cache

from psycopg import sql, Cursor, AsyncCursor
from psycopg.rows import dict_row
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool, ConnectionPool

from chancy.migrate import Migrator
from chancy.queue import Queue
from chancy.job import Reference, Job, QueuedJob, IsAJob
from chancy.plugin import Plugin
from chancy.utils import chancy_uuid, chunked, json_dumps


@cache
def setup_default_logger(level: int = logging.INFO):
    """
    Set up the default logger for the application.

    This method will configure the logger for the application, setting the
    log level and adding a stream handler to log to the console.

    :param level: The log level to set on the logger. Defaults to DEBUG.
    """
    logger = logging.getLogger("chancy")
    logger.setLevel(level)

    if logger.handlers:
        return logger

    try:
        from rich.logging import RichHandler

        handler = RichHandler()
    except ImportError:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(
                fmt="[%(asctime)s][%(levelname)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )

    logger.handlers[:] = [handler]
    return logger


class Chancy:
    """
    A Chancy application, along with all of its configuration and common
    functionality.

    Chancy is asyncio-first, and all of its methods are async by default:

    .. code-block:: python

        import asyncio
        from chancy import Chancy, Job, Queue

        def my_job():
            print("Hello, world!")

        async def main():
            async with Chancy("postgresql://localhost/chancy") as chancy:
                await chancy.migrate()
                await chancy.declare(Queue("default"))
                await chancy.push(Job.from_func(my_job))

        asyncio.run(main())

    Chancy does also provide a minimal synchronous interface for pushing jobs
    onto the queue for codebases that are not using asyncio:

    .. code-block:: python

        from chancy import Chancy, Job, Queue

        def my_job():
            print("Hello, world!")

        with Chancy("postgresql://localhost/chancy") as chancy:
            chancy.sync_push(Job.from_func(my_job))

    And of course, it's just Postgres under the hood, so you can always just
    insert jobs directly into the database if you're feeling brave.

    :param dsn: The DSN to connect to the database.
    :param plugins: The plugins to use with the application.
    :param prefix: A prefix appended to all table names, which can be used to
        namespace the tables for multiple applications or tenants.
    :param min_connection_pool_size: The minimum number of connections to keep
        in the pool.
    :param max_connection_pool_size: The maximum number of connections to keep
        in the pool.
    :param poll_reconnect_timeout: The number of seconds to wait before
        attempting to reconnect to the database if a connection is lost.
    :param notifications: Enables or disables emitting notifications using
        postgres's NOTIFY/LISTEN feature.
    :param log: The logger to use for all application logging. If not provided,
        a default logger will be set up.
    """

    class Executor(enum.StrEnum):
        """
        Shortcuts for the built-in executors.
        """

        #: The default executor, which runs jobs in a process pool.
        Process = "chancy.executors.process.ProcessExecutor"
        #: The threaded executor, which runs jobs in a thread pool.
        Threaded = "chancy.executors.thread.ThreadedExecutor"
        #: The asyncio executor, which runs jobs in an asyncio event loop.
        Async = "chancy.executors.asyncex.AsyncExecutor"
        #: The subprocess executor, which runs jobs in an experimental
        #: subinterpreter.
        SubInterpreter = "chancy.executors.sub.SubInterpreterExecutor"

    @staticmethod
    def _ensure_pool_is_open(f):
        """
        A decorator which ensures the connection pool is open before calling
        the wrapped function.

        It's never ideal to rely on this, since it will never _close_ the pool,
        but users expect to be able to just call push() and have it work.
        """

        @functools.wraps(f)
        async def wrapper(self, *args, **kwargs):
            await self.pool.open()
            return await f(self, *args, **kwargs)

        return wrapper

    @staticmethod
    def _ensure_sync_pool_is_open(f):
        """
        A decorator which ensures the connection pool is open before calling
        the wrapped function.

        It's never ideal to rely on this, since it will never _close_ the pool,
        but users expect to be able to just call push() and have it work.
        """

        @functools.wraps(f)
        def wrapper(self, *args, **kwargs):
            self.sync_pool.open()
            return f(self, *args, **kwargs)

        return wrapper

    @staticmethod
    def _ensure_pool_is_open_async_iter(f):
        """
        A decorator which ensures the connection pool is open before calling
        the wrapped function.

        It's never ideal to rely on this, since it will never _close_ the pool,
        but users expect to be able to just call push() and have it work.
        """

        @functools.wraps(f)
        async def wrapper(self, *args, **kwargs):
            await self.pool.open()
            async for record in f(self, *args, **kwargs):
                yield record

        return wrapper

    def __init__(
        self,
        dsn: str,
        *,
        plugins: list[Plugin] = None,
        prefix: str = "chancy_",
        min_connection_pool_size: int = 1,
        max_connection_pool_size: int = 10,
        poll_reconnect_timeout: int = 60 * 5,
        notifications: bool = True,
        log: logging.Logger | None = None,
    ):
        #: The DSN to connect to the database.
        self.dsn = dsn
        #: The plugins to use with the application.
        self.plugins = plugins or []
        #: A prefix appended to all table names, which can be used to
        #: namespace the tables for multiple applications or tenants.
        self.prefix = prefix
        #: The minimum number of connections to keep in the pool.
        self.min_connection_pool_size = min_connection_pool_size
        #: The maximum number of connections to keep in the pool.
        self.max_connection_pool_size = max_connection_pool_size
        #: The number of seconds to wait before attempting to reconnect to the
        #: database if a connection is lost.
        self.poll_reconnect_timeout = poll_reconnect_timeout
        #: Enables or disables emitting notifications using postgres's
        #: NOTIFY/LISTEN feature.
        self.notifications = notifications
        #: The logger to use for all application logging.
        self.log = log or setup_default_logger()

    async def __aenter__(self):
        await self.pool.open()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.pool.close()

    def __enter__(self):
        self.sync_pool.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.sync_pool.close()
        return False

    @cached_property
    def pool(self) -> AsyncConnectionPool:
        """
        The asyncio connection pool used to interact with the database.
        """
        return AsyncConnectionPool(
            self.dsn,
            min_size=self.min_connection_pool_size,
            max_size=self.max_connection_pool_size,
            reconnect_timeout=self.poll_reconnect_timeout,
            # Opening the pool here is deprecated, and should be done
            # explicitly by the user with a context manager or open/close.
            open=False,
        )

    @cached_property
    def sync_pool(self) -> ConnectionPool:
        """
        The synchronous connection pool used to interact with the database.
        """
        return ConnectionPool(
            self.dsn,
            min_size=self.min_connection_pool_size,
            max_size=self.max_connection_pool_size,
            reconnect_timeout=self.poll_reconnect_timeout,
        )

    @_ensure_pool_is_open
    async def migrate(self, *, to_version: int | None = None):
        """
        Migrate the database to the latest version.

        This will migrate the core database tables as well as any enabled
        plugins. If a version is provided, the database will be migrated up
        _or_ down to that version as necessary.

        .. code-block:: python

            async with Chancy("postgresql://localhost/chancy") as chancy:
                await chancy.migrate()

        Can also be run with the CLI:

        .. code-block:: bash

            chancy --app worker.chancy misc migrate

        .. warning::

            You should never run migrations in production without first testing
            them in a development environment. Migrations can be destructive and
            may cause data loss if not written carefully, or take a long time to
            complete if the database is large.

        :param to_version: The version to migrate to. If not provided, the
            database will be migrated to the latest version.
        """
        migrator = Migrator("chancy", "chancy.migrations", prefix=self.prefix)
        async with self.pool.connection() as conn:
            await migrator.migrate(conn, to_version=to_version)

        for plugin in self.plugins:
            await plugin.migrate(self, to_version=to_version)

    @_ensure_pool_is_open
    async def is_up_to_date(self) -> bool:
        """
        Check if the database is up to date.

        This will check if the database schema is up to date with the latest
        available migrations. If the database is up to date, returns `True`.
        """
        migrator = Migrator("chancy", "chancy.migrations", prefix=self.prefix)
        async with self.pool.connection() as conn:
            async with conn.cursor() as cursor:
                if await migrator.is_migration_required(cursor):
                    return False

                for plugin in self.plugins:
                    migrator = plugin.migrator(self)
                    if migrator is None:
                        continue
                    if await migrator.is_migration_required(cursor):
                        return False

        return True

    @_ensure_pool_is_open
    async def declare(self, queue: Queue, *, upsert: bool = False) -> Queue:
        """
        Declare a queue in the database.

        Queues in Chancy are the primary mechanism for organizing and
        executing jobs. They can be configured with various options to
        control how jobs are processed, including the concurrency level,
        rate limiting, and tags to restrict which workers can process jobs
        from the queue.

        Queues exist globally, configured at runtime via the database and
        can be updated, reassigned, or deleted at any time without requiring
        a restart of the workers.

        This will create a new queue in the database with the provided
        configuration. If the queue already exists, no changes will be
        made unless the ``upsert`` parameter is also set to ``True``.

        .. code-block:: python

            async with Chancy("postgresql://localhost/chancy") as chancy:
                await chancy.declare(Queue("default"))

        :param queue: The queue to declare.
        :param upsert: If `True`, the queue will be updated if it already
            exists. Defaults to `False`.
        :return: The queue as it exists in the database.
        """
        async with self.pool.connection() as conn:
            async with conn.cursor() as cursor:
                queue = await self.declare_ex(cursor, queue, upsert=upsert)
                await self.notify(cursor, "queue.declared", {"q": queue.name})
                return queue

    async def declare_ex(
        self, cursor: AsyncCursor, queue: Queue, *, upsert: bool = False
    ) -> Queue:
        """
        Declare a queue in the database using a specific cursor.

        This is a low-level method that allows for more control over the
        database connection and transaction management. It is recommended to
        use the higher-level `declare` method in most cases.

        :param cursor: The cursor to use for the operation.
        :param queue: The queue to declare.
        :param upsert: If `True`, the queue will be updated if it already
            exists. Defaults to `False`.
        """
        await cursor.execute(
            self._declare_sql(upsert),
            {
                "name": queue.name,
                "state": queue.state.value,
                "concurrency": queue.concurrency,
                "tags": list(queue.tags),
                "executor": queue.executor,
                "executor_options": Json(queue.executor_options),
                "polling_interval": queue.polling_interval,
                "rate_limit": queue.rate_limit,
                "rate_limit_window": queue.rate_limit_window,
            },
        )

        result = await cursor.fetchone()
        return Queue(
            name=queue.name,
            state=result[0],
            concurrency=result[1],
            tags=set(result[2]),
            polling_interval=result[3],
            executor=result[4],
            executor_options=result[5],
            rate_limit=result[6],
            rate_limit_window=result[7],
        )

    @_ensure_pool_is_open
    async def push(self, job: Job | IsAJob[..., Any]) -> Reference:
        """
        Push a job onto the queue.

        This method will push a job onto the queue, making it available for
        processing by workers. A :class:`Reference` object is returned that
        can be used to track the progress of the job and retrieve the result
        when it is complete.

        .. code-block:: python

            from chancy import Chancy, Job

            @job()
            def my_job():
                print("Hello, world!")

            async with Chancy("postgresql://localhost/chancy") as chancy:
                await chancy.push(my_job)

        .. seealso::

            :meth:`sync_push` for a synchronous version of this method.

        :param job: The job to push onto the queue.
        :return: A reference to the job in the queue.
        """
        async with self.pool.connection() as conn:
            async with conn.cursor() as cursor:
                return (await self.push_many_ex(cursor, [job]))[0]

    @_ensure_sync_pool_is_open
    def sync_push(self, job: Job) -> Reference:
        """
        Synchronously push a job onto the queue.

        This method provides a synchronous interface to push a job onto the
        queue.

        .. code-block:: python

            from chancy import Chancy, Job

            @job()
            def my_job():
                print("Hello, world!")

            with Chancy("postgresql://localhost/chancy") as chancy:
                chancy.sync_push(my_job)

        .. seealso::

            :meth:`push` for an asynchronous version of this method.

        :param job: The job to push onto the queue.
        :return: A reference to the job in the queue.
        """
        with self.sync_pool.connection() as conn:
            with conn.cursor() as cursor:
                return self.sync_push_many_ex(cursor, [job])[0]

    @_ensure_pool_is_open_async_iter
    async def push_many(
        self, jobs: list[Job], *, batch_size: int = 1000
    ) -> Iterator[list[Reference]]:
        """
        Push multiple jobs onto the queue.

        This method will push multiple jobs onto the queue, making them
        available for processing by workers. Jobs are committed in batches,
        with the size of each batch controlled by the `batch_size` parameter.
        Each batch is committed within its own transaction to limit peak
        memory usage.

        Returns an iterator of lists of references to the jobs in the queue,
        one list per batch:

        .. code-block:: python

            async for references in chancy.push_many(jobs):
                print(references)

        .. seealso::

            :meth:`sync_push_many` for a synchronous version of this method.

        :param jobs: The jobs to push onto the queue.
        :param batch_size: The number of jobs to push in each batch. Defaults
            to 1000.
        :return: An iterator of lists of references to the jobs in the queue.
        """
        async with self.pool.connection() as conn:
            async with conn.cursor() as cursor:
                for chunk in chunked(jobs, batch_size):
                    async with conn.transaction():
                        yield await self.push_many_ex(cursor, chunk)

    @_ensure_sync_pool_is_open
    def sync_push_many(
        self, jobs: list[Job | IsAJob[..., Any]], *, batch_size: int = 1000
    ) -> Iterator[list[Reference]]:
        """
        Synchronously push multiple jobs onto the queue.

        This method provides a synchronous interface to push multiple jobs onto
        the queue.

        Returns an iterator of lists of references to the jobs in the queue,
        one list per batch.

        .. seealso::

            :meth:`push_many` for an asynchronous version of this method.

        :param jobs: The jobs to push onto the queue.
        :param batch_size: The number of jobs to push in each batch. Defaults
            to 1000.
        :return: An iterator of lists of references to the jobs in the queue.
        """
        with self.sync_pool.connection() as conn:
            with conn.cursor() as cursor:
                for chunk in chunked(jobs, batch_size):
                    with conn.transaction():
                        yield self.sync_push_many_ex(cursor, chunk)

    async def push_many_ex(
        self, cursor: AsyncCursor, jobs: list[Job | IsAJob[..., Any]]
    ) -> list[Reference]:
        """
        Push multiple jobs onto the queue using a specific cursor.

        This is a low-level method that allows for more control over the
        database connection and transaction management. It is recommended to
        use the higher-level `push_many` method in most cases.

        The main advantage of using this function is to ensure your jobs are
        only pushed if some other operation is successful. For example, you
        only want to send an email confirmation if account creation actually
        succeeds. For example:

        .. seealso::

            :meth:`sync_push_many_ex` for a synchronous version of this method.

        :param cursor: The cursor to use for the operation.
        :param jobs: The jobs to push onto the queue.
        :return: A list of references to the jobs in the queue.
        """
        references = []
        for job in jobs:
            await cursor.execute(
                self._push_job_sql(),
                self._get_job_params(job),
            )
            record = await cursor.fetchone()
            references.append(Reference(record[0]))

        if self.notifications:
            for queue in set(
                job.queue if isinstance(job, Job) else job.job.queue
                for job in jobs
            ):
                await self.notify(cursor, "queue.pushed", {"q": queue})

        return references

    def sync_push_many_ex(
        self, cursor: Cursor, jobs: list[Job]
    ) -> list[Reference]:
        """
        Synchronously push multiple jobs onto the queue using a specific cursor.

        This is a low-level method that allows for more control over the
        database connection and transaction management. It is recommended to
        use the higher-level `sync_push_many` method in most cases.

        .. seealso::

            :meth:`push_many_ex` for an asynchronous version of this method.

        :param cursor: The cursor to use for the operation.
        :param jobs: The jobs to push onto the queue.
        :return: A list of references to the jobs in the queue.
        """
        references = []
        for job in jobs:
            cursor.execute(
                self._push_job_sql(),
                self._get_job_params(job),
            )
            record = cursor.fetchone()
            references.append(Reference(record[0]))

        for queue in set(job.queue for job in jobs):
            self.sync_notify(cursor, "queue.pushed", {"q": queue})

        return references

    @_ensure_pool_is_open
    async def get_job(self, ref: Reference) -> QueuedJob | None:
        """
        Resolve a reference to a job instance.

        If the job no longer exists, returns ``None``.

        :param ref: The reference to the job to retrieve.
        """
        async with self.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(self._get_job_sql(), [ref.identifier])
                record = await cursor.fetchone()
                if record is None:
                    return None
                return QueuedJob.unpack(record)

    async def wait_for_job(
        self,
        ref: Reference,
        *,
        interval: int = 1,
        timeout: float | int | None = None,
    ) -> QueuedJob | None:
        """
        Wait for a job to complete.

        This method will loop until the job referenced by the provided
        reference has completed. The interval parameter controls how often
        the job status is checked. This will not block the event loop, so
        other tasks can run while waiting for the job to complete.

        If the job no longer exists, returns ``None``.

        :param ref: The reference to the job to wait for.
        :param interval: The number of seconds to wait between checks.
        :param timeout: The maximum number of seconds to wait for the job to
            complete. If not provided, the method will wait indefinitely.
        """
        async with asyncio.timeout(timeout):
            while True:
                job = await self.get_job(ref)
                if job is None or job.state in {
                    QueuedJob.State.SUCCEEDED,
                    QueuedJob.State.FAILED,
                }:
                    return job
                await asyncio.sleep(interval)

    @_ensure_pool_is_open
    async def get_all_queues(self) -> list[Queue]:
        """
        Get all queues known to the cluster, regardless of their status
        or if they're assigned to any workers.
        """
        async with self.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(self._get_all_queues_sql())
                return [Queue.unpack(record) async for record in cursor]

    @_ensure_pool_is_open
    async def get_queue(self, name: str) -> Queue:
        """
        Get a specific queue by name.

        :param name: The name of the queue to retrieve.
        """
        async with self.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(self._get_queue_sql(), [name])
                record = await cursor.fetchone()
                if record is None:
                    raise KeyError(f"Queue {name!r} not found.")
                return Queue.unpack(record)

    async def delete_queue(self, name: str, *, purge_jobs: bool = True):
        """
        Delete a queue by name.

        By default, also deletes all jobs in the queue. If you want to keep
        the jobs, set `purge_jobs` to `False`.

        :param name: The name of the queue to delete.
        :param purge_jobs: If `True`, all jobs in the queue will be deleted
            along with the queue. If `False`, the jobs will be left in the
            database.
        """
        async with self.pool.connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    sql.SQL(
                        """
                        DELETE FROM {queues}
                        WHERE name = %s
                        """
                    ).format(queues=sql.Identifier(f"{self.prefix}queues")),
                    [name],
                )
                if purge_jobs:
                    await cursor.execute(
                        sql.SQL(
                            """
                            DELETE FROM {jobs}
                            WHERE queue = %s
                            """
                        ).format(jobs=sql.Identifier(f"{self.prefix}jobs")),
                        [name],
                    )

    @_ensure_pool_is_open
    async def get_all_workers(self) -> list[dict[str, Any]]:
        """
        Get all workers known to the cluster, regardless of their status.
        """
        async with self.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(self._get_all_workers_sql())
                return [record async for record in cursor]

    async def notify(
        self, cursor: AsyncCursor, event: str, payload: dict[str, Any]
    ):
        """
        Send a notification via Postgres NOTIFY/LISTEN to all other listening
        workers.

        If notifications are disabled on the associated Chancy app, this
        method will do nothing.

        .. seealso::

            :meth:`sync_notify` for a synchronous version of this method.
        """
        if not self.notifications:
            return

        await cursor.execute(
            "SELECT pg_notify(%s, %s)",
            [
                f"{self.prefix}events",
                json_dumps({"t": event, **payload}),
            ],
        )

    def sync_notify(self, cursor: Cursor, event: str, payload: dict[str, Any]):
        """
        Send a notification via Postgres NOTIFY/LISTEN to all other listening
        workers.

        If notifications are disabled on the associated Chancy app, this
        method will do nothing.

        .. seealso::

            :meth:`notify` for an asynchronous version of this method.
        """
        if not self.notifications:
            return

        cursor.execute(
            "SELECT pg_notify(%s, %s)",
            [
                f"{self.prefix}events",
                json_dumps({"t": event, **payload}),
            ],
        )

    async def cancel_job(self, ref: Reference):
        """
        Cancel a job by reference.

        This will attempt to cancel a job that is currently running, if it
        is possible to do so. Notifications must be enabled for this to work.

        :param ref: The reference to the job to cancel.
        """
        async with self.pool.connection() as conn:
            async with conn.cursor() as cursor:
                async with conn.transaction():
                    await cursor.execute(
                        sql.SQL(
                            """
                            UPDATE {jobs}
                            SET state = 'failed'
                            WHERE id = %s
                            """
                        ).format(jobs=sql.Identifier(f"{self.prefix}jobs")),
                        [ref.identifier],
                    )

                    # Tell any workers that might have already snagged the job
                    # to cancel it.
                    await self.notify(
                        cursor, "job.cancelled", {"j": ref.identifier}
                    )

    def _get_all_workers_sql(self):
        return sql.SQL(
            """
            SELECT
                wr.*,
                l.worker_id IS NOT NULL as is_leader
            FROM
                {workers} wr
            LEFT JOIN
                {leader} l on l.worker_id = wr.worker_id
            """
        ).format(
            workers=sql.Identifier(f"{self.prefix}workers"),
            leader=sql.Identifier(f"{self.prefix}leader"),
        )

    def _get_queue_sql(self):
        return sql.SQL(
            """
            SELECT
                *
            FROM
                {queues}
            WHERE
                name = %s
            """
        ).format(queues=sql.Identifier(f"{self.prefix}queues"))

    def _get_all_queues_sql(self):
        return sql.SQL(
            """
            SELECT
                *
            FROM
                {queues}
            """
        ).format(queues=sql.Identifier(f"{self.prefix}queues"))

    def _push_job_sql(self):
        return sql.SQL(
            """
            INSERT INTO
                {jobs} (
                    id,
                    queue,
                    func,
                    kwargs,
                    limits,
                    meta,
                    priority,
                    max_attempts,
                    scheduled_at,
                    unique_key
                )
            VALUES (
                %(id)s,
                %(queue)s,
                %(func)s,
                %(kwargs)s,
                %(limits)s,
                %(meta)s,
                %(priority)s,
                %(max_attempts)s,
                %(scheduled_at)s,
                %(unique_key)s
            )
            ON CONFLICT (unique_key)
            WHERE
                unique_key IS NOT NULL
                    AND state NOT IN ('succeeded', 'failed')
            DO UPDATE
               SET
                   state = EXCLUDED.state
            RETURNING id;
            """
        ).format(jobs=sql.Identifier(f"{self.prefix}jobs"))

    def _get_job_sql(self):
        return sql.SQL(
            """
            SELECT
                *
            FROM
                {jobs}
            WHERE
                id = %s
            """
        ).format(jobs=sql.Identifier(f"{self.prefix}jobs"))

    def _declare_sql(self, upsert: bool):
        action = sql.SQL(
            """
            UPDATE SET
                name = EXCLUDED.name
            """
        )
        if upsert:
            action = sql.SQL(
                """
                UPDATE SET
                    state = EXCLUDED.state,
                    concurrency = EXCLUDED.concurrency,
                    tags = EXCLUDED.tags,
                    executor = EXCLUDED.executor,
                    executor_options = EXCLUDED.executor_options,
                    polling_interval = EXCLUDED.polling_interval,
                    rate_limit = EXCLUDED.rate_limit,
                    rate_limit_window = EXCLUDED.rate_limit_window
                """
            )

        return sql.SQL(
            """
            INSERT INTO {queues} (
                name,
                state,
                concurrency,
                tags,
                executor,
                executor_options,
                polling_interval,
                rate_limit,
                rate_limit_window
            ) VALUES (
                %(name)s,
                %(state)s,
                %(concurrency)s,
                %(tags)s,
                %(executor)s,
                %(executor_options)s,
                %(polling_interval)s,
                %(rate_limit)s,
                %(rate_limit_window)s
            )
            ON CONFLICT (name) DO
                {action}
            RETURNING 
                state,
                concurrency,
                tags,
                polling_interval,
                executor,
                executor_options,
                rate_limit,
                rate_limit_window;
            """
        ).format(
            queues=sql.Identifier(f"{self.prefix}queues"),
            action=action,
        )

    @staticmethod
    def _get_job_params(job: Job | IsAJob[..., Any]) -> dict:
        """
        Get the parameters for a job to be inserted into the database.

        :param job: The job to get parameters for.
        :return: A dictionary of parameters for the job.
        """
        if callable(job):
            job = job.job

        return {
            "id": chancy_uuid(),
            "queue": job.queue,
            "func": job.func,
            "kwargs": Json(job.kwargs or {}),
            "limits": Json([limit.serialize() for limit in job.limits]),
            "meta": Json(job.meta),
            "priority": job.priority,
            "max_attempts": job.max_attempts,
            "scheduled_at": job.scheduled_at,
            "unique_key": job.unique_key,
        }
