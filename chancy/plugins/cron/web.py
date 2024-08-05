from psycopg import sql
from psycopg.rows import dict_row
from starlette.responses import Response

from chancy import Chancy, Worker, Job
from chancy.plugins.web import WebPlugin


class CronWebPlugin(WebPlugin):
    def name(self):
        return "cron"

    def links(self) -> list[tuple[str, str]]:
        return [("Schedules", "/schedules")]

    def routes(self):
        return [
            {
                "path": "/schedules",
                "endpoint": self.schedules_view,
                "methods": ["GET"],
                "name": "schedules",
            },
            {
                "path": "/schedule/{unique_key}",
                "endpoint": self.schedule_view,
                "methods": ["GET"],
                "name": "schedule",
            },
        ]

    async def schedules_view(self, request, chancy: Chancy, worker: Worker):
        async with chancy.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    sql.SQL(
                        """
                        SELECT
                            unique_key,
                            cron,
                            job,
                            next_run,
                            last_run
                        FROM {table}
                        ORDER BY next_run
                        """
                    ).format(table=sql.Identifier(f"{chancy.prefix}cron"))
                )
                schedules = await cursor.fetchall()

        return Response(
            await self.render(
                "schedules.html",
                request,
                schedules=[
                    {**schedule, "job": Job.unpack(schedule["job"])}
                    for schedule in schedules
                ],
            ),
            media_type="text/html",
        )

    async def schedule_view(self, request, chancy: Chancy, worker: Worker):
        unique_key = request.path_params["unique_key"]
        async with chancy.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    sql.SQL(
                        """
                        SELECT
                            unique_key,
                            cron,
                            job,
                            next_run,
                            last_run
                        FROM {table}
                        WHERE unique_key = %(unique_key)s
                        """
                    ).format(table=sql.Identifier(f"{chancy.prefix}cron")),
                    {"unique_key": unique_key},
                )
                schedule = await cursor.fetchone()

        if not schedule:
            return Response("Schedule not found.", status_code=404)

        return Response(
            await self.render(
                "schedule.html",
                request,
                schedule={**schedule, "job": Job.unpack(schedule["job"])},
            ),
            media_type="text/html",
        )
