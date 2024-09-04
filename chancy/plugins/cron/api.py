from dataclasses import asdict

from psycopg.rows import dict_row
from starlette.responses import Response

from chancy import Job
from chancy.plugins.api import ApiPlugin
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
    async def get_cron(request, *, chancy, worker):
        """
        Get all known cron jobs.
        """
        async with chancy.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    """
                    SELECT * FROM {table}
                    """.format(
                        table=f"{chancy.prefix}cron"
                    )
                )

                results = await cursor.fetchall()

                return Response(
                    json_dumps(
                        [
                            {
                                **result,
                                "job": asdict(Job.unpack(result["job"])),
                            }
                            for result in results
                        ]
                    ),
                    media_type="application/json",
                )
