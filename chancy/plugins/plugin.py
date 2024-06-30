import abc
import enum
import asyncio
import typing

if typing.TYPE_CHECKING:
    from chancy.app import Chancy
    from chancy.worker import Worker


class PluginScope(enum.Enum):
    WORKER = "worker"


class Plugin(abc.ABC):
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

    def __repr__(self):
        return f"<{self.__class__.__name__}()>"
