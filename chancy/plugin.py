import abc
import enum
import asyncio
import typing
import logging
from functools import cached_property

from chancy.logger import logger, PrefixAdapter
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
        #: An asyncio.Event that can be used to cancel the plugin.
        self.cancel_signal = asyncio.Event()
        #: An asyncio.Event that can be used to wake up the plugin if it's
        #: sleeping.
        self.wakeup_signal = asyncio.Event()

    async def cancel(self) -> None:
        """
        Cancel the plugin.

        Wakes up the plugin if it's sleeping and attempts to have it exit
        gracefully.
        """
        self.cancel_signal.set()

    @cached_property
    def log(self) -> logging.LoggerAdapter:
        return PrefixAdapter(logger, {"prefix": self.__class__.__name__})

    @classmethod
    @abc.abstractmethod
    def get_scope(cls) -> PluginScope:
        """
        Get the scope of this plugin.
        """

    @abc.abstractmethod
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
        cancelled during the sleep by calling :meth:`cancel`.
        """
        sleep = asyncio.create_task(asyncio.sleep(seconds))
        cancel = asyncio.create_task(self.cancel_signal.wait())
        wakeup = asyncio.create_task(self.wakeup_signal.wait())

        try:
            done, pending = await asyncio.wait(
                [sleep, cancel, wakeup],
                return_when=asyncio.FIRST_COMPLETED,
            )

            if cancel in done:
                return False
        finally:
            for task in [sleep, cancel, wakeup]:
                if not task.done():
                    task.cancel()

            await asyncio.gather(sleep, cancel, wakeup, return_exceptions=True)
            self.wakeup_signal.clear()

        return True

    async def wait_for_leader(self, worker: "Worker") -> None:
        """
        Wait until the worker running this plugin is the leader.

        This function will return immediately if the worker is already the
        leader. If the plugin's :meth:`cancel` method is called, this function
        will raise an asyncio.CancelledError.
        """
        cancel = asyncio.create_task(self.cancel_signal.wait())
        leader = asyncio.create_task(worker.is_leader.wait())

        done, pending = await asyncio.wait(
            [cancel, leader],
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()

        if cancel in done:
            raise asyncio.CancelledError()

    def wake_up(self):
        self.wakeup_signal.set()

    def __repr__(self):
        return f"<{self.__class__.__name__}()>"
