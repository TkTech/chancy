from psycopg import sql
from psycopg.rows import dict_row
from starlette.authentication import requires
from starlette.requests import Request
from starlette.responses import Response

from chancy.plugins.api.plugin import ApiPlugin
from chancy.rule import JobRules
from chancy.utils import json_dumps


class CoreApiPlugin(ApiPlugin):
    """
    Core API plugin which implements the endpoints for jobs, queues, workers,
    etc...
    """

    def name(self):
        return "base"

    def routes(self):
        return [
            {
                "path": "/api/v1/configuration",
                "endpoint": self.get_configuration,
                "methods": ["GET"],
                "name": "get_configuration",
            },
            {
                "path": "/api/v1/login",
                "endpoint": self.login,
                "methods": ["POST"],
                "name": "login",
            },
            {
                "path": "/api/v1/queues",
                "endpoint": self.get_queues,
                "methods": ["GET"],
                "name": "get_queues",
            },
            {
                "path": "/api/v1/workers",
                "endpoint": self.get_workers,
                "methods": ["GET"],
                "name": "get_workers",
            },
            {
                "path": "/api/v1/jobs",
                "endpoint": self.get_jobs,
                "methods": ["GET"],
                "name": "get_jobs",
            },
            {
                "path": "/api/v1/jobs/{id}",
                "endpoint": self.get_job,
                "methods": ["GET"],
                "name": "get_job",
            },
        ]

    async def login(self, request: Request, *, chancy, worker):
        """
        Login endpoint.
        """
        data = await request.json()

        username = data.get("username")
        password = data.get("password")

        if not username or not password:
            return Response(
                json_dumps({"error": "Invalid username or password"}),
                status_code=401,
                media_type="application/json",
            )

        if not await self.api.authentication_backend.login(
            request, username, password
        ):
            return Response(
                json_dumps({"error": "Invalid username or password"}),
                status_code=401,
                media_type="application/json",
            )

        return Response(
            json_dumps({"success": "Authenticated"}),
            media_type="application/json",
        )

    @staticmethod
    @requires(["authenticated"])
    async def get_configuration(request, *, chancy, worker):
        """
        Get the configuration of the Chancy instance.
        """
        return Response(
            json_dumps(
                {
                    "plugins": [
                        plugin.__class__.__name__
                        for plugin in chancy.plugins.values()
                    ]
                }
            ),
            media_type="application/json",
        )

    @staticmethod
    @requires(["authenticated"])
    async def get_queues(request, *, chancy, worker):
        """
        Get a list of all the queues.
        """
        queues = await chancy.get_all_queues()
        return Response(
            json_dumps([queue.pack() for queue in queues]),
            media_type="application/json",
        )

    @staticmethod
    @requires(["authenticated"])
    async def get_workers(request, *, chancy, worker):
        """
        Get a list of all the workers.
        """
        workers = await chancy.get_all_workers()
        return Response(
            json_dumps(workers),
            media_type="application/json",
        )

    @staticmethod
    @requires(["authenticated"])
    async def get_jobs(request, *, chancy, worker):
        """
        Get a list of all the jobs in the system.

        Allows ID-based pagination and basic filtering.
        """
        state = request.query_params.get("state")
        queue = request.query_params.get("queue")
        func = request.query_params.get("func")
        limit = min(int(request.query_params.get("limit", 100)), 100)
        before = request.query_params.get("before")

        rule = JobRules.All()

        if state:
            rule = JobRules.State() == state

        if before:
            rule &= JobRules.ID() < before

        if queue:
            rule &= JobRules.Queue() == queue

        if func:
            rule &= JobRules.Job().contains(func)

        async with chancy.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    sql.SQL(
                        """
                        SELECT * FROM {jobs}
                        WHERE ({rule})
                        ORDER BY (
                            completed_at,
                            scheduled_at 
                        ) DESC
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
    @requires(["authenticated"])
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
