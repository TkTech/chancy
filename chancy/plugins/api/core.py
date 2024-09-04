from psycopg import sql
from psycopg.rows import dict_row
from starlette.responses import Response

from chancy.plugins.api.plugin import ApiPlugin
from chancy.rule import JobRules
from chancy.utils import json_dumps


class CoreApiPlugin(ApiPlugin):
    def name(self):
        return "base"

    def routes(self):
        return [
            {
                "path": "/configuration",
                "endpoint": self.get_configuration,
                "methods": ["GET"],
                "name": "get_configuration",
            },
            {
                "path": "/queues",
                "endpoint": self.get_queues,
                "methods": ["GET"],
                "name": "get_queues",
            },
            {
                "path": "/workers",
                "endpoint": self.get_workers,
                "methods": ["GET"],
                "name": "get_workers",
            },
            {
                "path": "/jobs",
                "endpoint": self.get_jobs,
                "methods": ["GET"],
                "name": "get_jobs",
            },
            {
                "path": "/jobs/{id}",
                "endpoint": self.get_job,
                "methods": ["GET"],
                "name": "get_job",
            },
        ]

    @staticmethod
    async def get_configuration(request, *, chancy, worker):
        """
        Get the configuration of the Chancy instance.
        """
        return Response(
            json_dumps(
                {
                    "plugins": [
                        plugin.__class__.__name__ for plugin in chancy.plugins
                    ]
                }
            ),
            media_type="application/json",
        )

    @staticmethod
    async def get_queues(request, *, chancy, worker):
        """
        Get a list of all the queues in the system.
        """
        queues = await chancy.get_all_queues()
        return Response(
            json_dumps([queue.pack() for queue in queues]),
            media_type="application/json",
        )

    @staticmethod
    async def get_workers(request, *, chancy, worker):
        """
        Get a list of all the workers in the system.
        """
        workers = await chancy.get_all_workers()
        return Response(
            json_dumps(workers),
            media_type="application/json",
        )

    @staticmethod
    async def get_jobs(request, *, chancy, worker):
        """
        Get a list of all the jobs in the system.

        Allows ID-based pagination and basic filtering.
        """
        state = request.query_params.get("state")
        queue = request.query_params.get("queue")
        limit = min(int(request.query_params.get("limit", 100)), 100)
        before = request.query_params.get("before")

        rule = JobRules.State() == "pending"

        if state:
            rule = JobRules.State() == state

        if before:
            rule &= JobRules.ID() < before

        if queue:
            rule &= JobRules.Queue() == queue

        async with chancy.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    sql.SQL(
                        """
                        SELECT * FROM {jobs}
                        WHERE ({rule})
                        ORDER BY id DESC
                        LIMIT {limit}
                        """
                    ).format(
                        jobs=sql.Identifier(f"{chancy.prefix}jobs"),
                        rule=rule.to_sql(),
                        limit=sql.Literal(limit),
                    )
                )

                result = await cursor.fetchall()

                if not result:
                    return Response(
                        json_dumps([]),
                        media_type="application/json",
                    )

                return Response(
                    json_dumps(result),
                    media_type="application/json",
                )

    @staticmethod
    async def get_job(request, *, chancy, worker):
        """
        Get a single job by ID.
        """
        job_id = request.path_params["id"]

        async with chancy.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    sql.SQL(
                        """
                        SELECT * FROM {jobs}
                        WHERE id = %(job_id)s
                        """
                    ).format(
                        jobs=sql.Identifier(f"{chancy.prefix}jobs"),
                    ),
                    {"job_id": job_id},
                )

                result = await cursor.fetchone()

                if not result:
                    return Response(
                        json_dumps({}),
                        status_code=404,
                        media_type="application/json",
                    )

                return Response(
                    json_dumps(result),
                    media_type="application/json",
                )
