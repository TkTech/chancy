from functools import partial

from chancy.hub import Event
from chancy.plugin import Plugin
from chancy.session import PluginSession
from chancy.worker import Worker


class DebugPlugin(Plugin):
    """
    Logs additional job information, such as exceptions.
    """

    def __init__(self):
        super().__init__()
        self.event_counts = {}

    async def setup(self, session: PluginSession):
        session.subscribe(None, partial(self._log_any, session.worker))

    def _log_any(self, worker: Worker, event: Event):
        worker.chancy.log.info("Event %s: %s", event.name, event.body)
        self.event_counts[event.name] = self.event_counts.get(event.name, 0) + 1

    def get_event_count(self, event_name):
        return self.event_counts.get(event_name, 0)

    @staticmethod
    def get_identifier() -> str:
        return "chancy.debug_plugin"
