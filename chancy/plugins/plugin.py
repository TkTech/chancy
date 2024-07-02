import abc
import enum
import asyncio
import typing

if typing.TYPE_CHECKING:
    from chancy.app import Chancy
    from chancy.worker import Worker


class PluginScope(enum.Enum):
    #: The plugin will run only on workers.
    WORKER = "worker"


class Plugin(abc.ABC):
    """
    Base class for all plugins.

    Plugins are used to extend the functionality of the worker. When a worker
    starts, it will call :meth:`run` on all plugins that have a scope that
    matches the worker's scope.

    """

    def __init__(self):
        # An asyncio.Event that can be used to cancel the plugin.
        self.cancel_signal = asyncio.Event()

    async def cancel(self) -> None:
        """
        Cancel the plugin.
        """
        self.cancel_signal.set()

    async def is_cancelled(self) -> bool:
        """
        Check if the plugin has been cancelled.
        """
        return self.cancel_signal.is_set()

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

    async def sleep(self, seconds: int) -> None:
        """
        Sleep for a specified number of seconds, but allow the plugin to be
        cancelled during the sleep by calling :meth:`cancel`.
        """
        await asyncio.wait(
            # As of 3.11, asyncio.wait() no longer accepts coroutines directly,
            # so we must wrap them in asyncio.create_task().
            [
                asyncio.create_task(asyncio.sleep(seconds)),
                asyncio.create_task(self.cancel_signal.wait()),
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )

    def __repr__(self):
        return f"<{self.__class__.__name__}()>"
