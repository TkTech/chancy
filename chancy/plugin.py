import abc
import enum
import asyncio
import typing

from chancy.job import QueuedJob
from chancy.migrate import Migrator

if typing.TYPE_CHECKING:
    from chancy.app import Chancy
    from chancy.worker import Worker


class PluginScope(enum.Enum):
    #: The plugin is a general worker plugin.
    WORKER = "worker"


class Plugin(abc.ABC):
    """
    Base class for all plugins.

    Plugins are used to extend the functionality of the worker. When a worker
    starts, it will call :meth:`run` on all plugins that have a scope that
    matches the worker's scope.
    """

    def __init__(self):
        #: An asyncio.Event that can be used to wake up the plugin if it's
        #: sleeping.
        self.wakeup_signal = asyncio.Event()

    @classmethod
    @abc.abstractmethod
    def get_scope(cls) -> PluginScope:
        """
        Get the scope of this plugin.
        """

    async def run(self, worker: "Worker", chancy: "Chancy"):
        """
        Runs the plugin.

        This function can and should run indefinitely, as it will be cancelled
        when the worker is stopped.
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
            return

        package = self.migrate_package()
        if package is None:
            return

        return Migrator(key, package, prefix=chancy.prefix)

    async def sleep(self, seconds: int) -> bool:
        """
        Sleep for a specified number of seconds, but allow the plugin to be
        woken up early.
        """
        wakeup = asyncio.create_task(self.wakeup_signal.wait())

        done, pending = await asyncio.wait(
            [wakeup],
            timeout=seconds,
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()

        self.wakeup_signal.clear()
        return True

    @staticmethod
    async def wait_for_leader(worker: "Worker") -> None:
        """
        Wait until the worker running this plugin is the leader.
        """
        return await worker.is_leader.wait()

    def wake_up(self):
        """
        Wake up the plugin if it's sleeping.
        """
        self.wakeup_signal.set()

    async def cleanup(self, chancy: "Chancy") -> int | None:
        """
        Clean up any resources used by the plugin.

        Should return either None, if no work was done, or the number of
        rows cleaned up.

        .. note::

            Normally, you don't need to call this yourself. The Pruner plugin
            will call the cleanup method of all other registered plugins.
        """

    def api_plugin(self) -> str | None:
        """
        If this plugin has an associated API component, returns the import
        string for the plugin.
        """

    async def on_job_completed(
        self,
        job: QueuedJob,
        worker: "Worker",
        *,
        exc: Exception | None = None,
    ) -> QueuedJob:
        """
        Called after a job is completed (successfully or otherwise) and before
        the QueuedJob is updated in the database.

        If an exception occurred during the job, `exc` will be the exception
        instance instead of ``None``.

        The passed job is immutable - to modify it, return a new QueuedJob
        object with the desired changes.
        """
        raise NotImplementedError()

    def __repr__(self):
        return f"<{self.__class__.__name__}()>"
