import abc

from chancy.hub import Hub


class Plugin(abc.ABC):
    class Type:
        LEADER = "leader"
        WORKER = "worker"

    @classmethod
    @abc.abstractmethod
    def get_type(cls):
        """
        Returns the type of the plugin.
        """

    async def on_startup(self, hub: Hub):
        """
        Called when the plugin is started.
        """

    async def on_shutdown(self, hub: Hub):
        """
        Called when the plugin is stopped.
        """

    def __repr__(self):
        return f"<{self.__class__.__name__}>"
