import abc
import enum
import typing
from collections.abc import Sequence
from typing import Any

from psycopg import AsyncCursor
from psycopg.rows import DictRow

from chancy.job import QueuedJob
from chancy.migrate import Migrator

if typing.TYPE_CHECKING:
    from chancy.app import Chancy
    from chancy.session import PluginSession
    from chancy.worker import Worker


class PluginScope(enum.Enum):
    #: The plugin is a general worker plugin.
    WORKER = "worker"


class Plugin(abc.ABC):
    """
    Base class for all plugins.

    Plugins are used to extend the functionality of the worker and most of
    Chancy's features are implemented as swappable plugins. Writing and using
    plugins is fairly simple. For example:

    .. code-block:: python

        import asyncio

        from psycopg import sql

        from chancy import Chancy, Worker
        from chancy.plugin import Plugin

        class PendingJobs(Plugin):
            @staticmethod
            def get_identifier():
                return "myapp.pending_jobs"

            async def setup(self, session):
                session.periodic("report", self.report, interval=60)

            async def report(self, ctx):
                async def count_pending(tx):
                    await tx.cursor.execute(
                        sql.SQL(
                            "SELECT count(*) AS count FROM {} WHERE state = 'pending'"
                        ).format(sql.Identifier(f"{ctx.chancy.prefix}jobs"))
                    )
                    return (await tx.cursor.fetchone())["count"]

                count = await ctx.database(count_pending)
                ctx.log.info("Pending jobs: %s", count)

        async def main():
            async with Chancy(
                "postgresql://localhost/postgres",
                plugins=[PendingJobs()],
            ) as chancy:
                await chancy.migrate()
                async with Worker(chancy) as worker:
                    await worker.wait_for_shutdown()

        if __name__ == "__main__":
            asyncio.run(main())

    You're free to do whatever you want within a ``Plugin``, but we recommend
    using :class:`~chancy.session.PluginSession` to schedule work and manage
    resources, and :meth:`~chancy.session.OperationContext.database` for database
    operations. These helpers handle shutdown and retries after transient
    database errors. Database callbacks may run more than once, so they must
    be safe to repeat.
    """

    @classmethod
    def get_scope(cls) -> PluginScope:
        """
        Get the scope of this plugin. Scopes control when and where the plugin
        will be run.

        By default, plugins are scoped to the worker.
        """
        return PluginScope.WORKER

    async def setup(self, session: "PluginSession") -> None:
        """
        Register work and subscriptions for one worker.

        Called once before background work starts. An exception aborts worker
        startup and closes all sessions created so far.

        :param session: The worker's session for this plugin.
        """

    def migrate_package(self) -> str | None:
        """
        Get the package name that contains the migration scripts for this
        plugin, if it has any.
        """

    def migrate_key(self) -> str | None:
        """
        Get the migration key for this plugin, if it has any.
        """

    async def migrate(self, chancy: "Chancy", *, to_version: int | None = None):
        """
        Migrate the database to the latest schema version.

        If `to_version` is provided, the database will be migrated to that
        specific version, up or down as necessary.
        """
        migrator = self.migrator(chancy)
        if migrator is None:
            return

        async with chancy.pool.connection() as conn:
            await migrator.migrate(conn, to_version=to_version)

    def migrator(self, chancy: "Chancy") -> Migrator | None:
        """
        Get a migrator for this plugin, if it has any migration scripts.
        """
        key = self.migrate_key()
        if key is None:
            return None

        package = self.migrate_package()
        if package is None:
            return None

        return Migrator(key, package, prefix=chancy.prefix)

    async def cleanup(
        self, chancy: "Chancy", cursor: AsyncCursor[DictRow]
    ) -> int | None:
        """
        Remove expired data using the supplied transaction.

        Pruner calls this hook periodically. It may repeat after a connection
        failure, so repeated calls must be safe. Do not commit the transaction.

        :param chancy: The Chancy application.
        :param cursor: The cursor to use for database operations.
        :return: The number of rows removed, or None if no work was done.
        """

    def api_plugin(self) -> str | None:
        """
        If this plugin has an associated API component, returns the import
        string for the plugin.
        """

    async def on_job_starting(
        self,
        *,
        job: QueuedJob,
        worker: "Worker",
    ) -> QueuedJob:
        """
        Called when a job has been retrieved from the queue and is about to
        start.

        The passed job is immutable - to modify it, return a new QueuedJob
        with the desired changes.
        """
        raise NotImplementedError()

    async def on_job_completed(
        self,
        *,
        worker: "Worker",
        job: QueuedJob,
        exc: Exception | None = None,
        result: Any | None = None,
    ) -> QueuedJob:
        """
        Called after a job is completed (successfully or otherwise) and before
        the QueuedJob is updated in the database.

        If an exception occurred during the job, `exc` will be the exception
        instance instead of ``None``.

        The passed job is immutable - to modify it, return a new QueuedJob
        object with the desired changes.

        :param worker: The worker that is running the job.
        :param job: The job that was completed.
        :param exc: The exception that was raised, if any.
        :param result: The result of the job, if any.
        :return: The job to update in the database.
        """
        raise NotImplementedError()

    async def on_jobs_updated_in_transaction(
        self,
        *,
        worker: "Worker",
        jobs: Sequence[QueuedJob],
        cursor: AsyncCursor[DictRow],
    ):
        """
        Called after a batch's SQL updates, before its transaction commits.

        Use the supplied cursor for database work that must commit atomically
        with the job updates. Raising an exception rolls back the batch. This
        hook may run again on retry; avoid external or in-memory side effects.
        Use :meth:`~chancy.plugin.Plugin.on_job_updated` for work that requires
        committed updates.

        :param worker: The worker saving the updates.
        :param jobs: Only updates whose claims matched and whose database rows
                     were updated, in their original order, including repeated
                     accepted updates to the same job. Rejected updates are
                     omitted; this hook is not called if none were accepted.
        :param cursor: The cursor for the current update transaction.
        """

    async def on_worker_stopped(self, *, worker: "Worker"):
        """
        Called during shutdown, after the worker saves its final job updates.

        Use this hook to save remaining plugin data and release resources.
        Background tasks have already stopped. If shutdown fails and is retried,
        this hook may be called again, so repeated calls must be safe.

        :param worker: The worker being stopped.
        """

    async def on_job_updated(
        self,
        *,
        worker: "Worker",
        job: QueuedJob,
    ):
        """
        Called for each accepted job update after its transaction commits.

        This method cannot modify the saved update. Updates rejected because
        their claims no longer match or their jobs no longer exist do not
        invoke this hook. Accepted updates retain their original batch order,
        including repeated updates to the same job.

        :param worker: The worker saving the update.
        :param job: The accepted update that was committed.
        """

    def get_tables(self) -> list[str]:
        """
        Get the names of all tables this plugin is responsible for.

        By default, returns an empty list.
        """
        return []

    @staticmethod
    @abc.abstractmethod
    def get_identifier() -> str:
        """
        Returns a unique identifier for this plugin.

        This identifier should be unique across all active plugins. If a
        custom plugin provides compatible functionality to a built-in plugin,
        it may use the same identifier as the built-in plugin.
        """

    @staticmethod
    def get_dependencies() -> list[str]:
        """
        Get the identifiers of all plugins this plugin depends on, if any.

        Plugins that depend on the presence of other plugins will refuse to
        start if those dependencies are not met.
        """
        return []

    def __repr__(self):
        return f"<{self.__class__.__name__}()>"
