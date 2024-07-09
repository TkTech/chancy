import abc
import enum
import asyncio
import typing
import logging
from functools import cached_property

from chancy.logger import logger, PrefixAdapter

if typing.TYPE_CHECKING:
    from chancy.app import Chancy
    from chancy.worker import Worker


class PluginScope(enum.Enum):
    #: The plugin is a general worker plugin.
    WORKER = "worker"
    #: The plugin implements a queue provider.
    QUEUE = "queue"


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

    async def sleep(self, seconds: int) -> bool:
        """
        Sleep for a specified number of seconds, but allow the plugin to be
        cancelled during the sleep by calling :meth:`cancel`.
        """
        sleep = asyncio.create_task(asyncio.sleep(seconds))
        cancel = asyncio.create_task(self.cancel_signal.wait())
        wakeup = asyncio.create_task(self.wakeup_signal.wait())

        done, pending = await asyncio.wait(
            # As of 3.11, asyncio.wait() no longer accepts coroutines directly,
            # so we must wrap them in asyncio.create_task().
            [sleep, cancel, wakeup],
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()

        if cancel in done:
            raise asyncio.CancelledError()

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
