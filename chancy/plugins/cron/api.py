from starlette.authentication import requires
from starlette.responses import Response

from chancy.plugins.api import ApiPlugin
from chancy.plugins.cron import Cron
from chancy.utils import json_dumps


class CronApiPlugin(ApiPlugin):
    """
    Provides API endpoints for viewing cron jobs.
    """

    def name(self):
        return "cron"

    def routes(self):
        return [
            {
                "path": "/api/v1/crons",
                "endpoint": self.get_cron,
                "methods": ["GET"],
                "name": "get_cron",
            },
        ]

    @staticmethod
    @requires(["authenticated"])
    async def get_cron(request, *, chancy, worker):
        """
        Get all known cron jobs.
        """
        plugin: Cron = chancy.plugins["chancy.cron"]

        return Response(
            json_dumps(list((await plugin.get_schedules(chancy)).values())),
            media_type="application/json",
        )
