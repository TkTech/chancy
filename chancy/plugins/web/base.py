from psycopg import sql
from psycopg.rows import dict_row
from starlette.requests import Request
from starlette.responses import RedirectResponse, HTMLResponse

from chancy import Chancy, Worker
from chancy.rule import JobRules
from chancy.plugins.web.plugin import WebPlugin
from chancy.job import JobInstance


class BaseWebPlugin(WebPlugin):
    """
    A WebPlugin that implements views for core Chancy functionality, such
    as viewing jobs, workers, and queues.
    """

    def name(self):
        return "base"

    def links(self) -> list[tuple[str, str]]:
        return [
            ("Jobs", "/"),
            ("Workers", "/workers"),
            ("Queues", "/queues"),
        ]

    def routes(self):
        return [
            {
                "path": "/",
                "endpoint": self.index_view,
                "methods": ["GET"],
                "name": "index",
            },
            {
                "path": "/workers",
                "endpoint": self.workers_view,
                "methods": ["GET"],
                "name": "workers",
            },
            {
                "path": "/queues",
                "endpoint": self.queues_view,
                "methods": ["GET"],
                "name": "queues",
            },
            {
                "path": "/jobs",
                "endpoint": self.jobs_view,
                "methods": ["GET"],
                "name": "jobs",
            },
            {
                "path": "/jobs/{job_id}",
                "endpoint": self.job_view,
                "methods": ["GET"],
                "name": "job",
            },
        ]

    async def index_view(
        self, request: Request, chancy: Chancy, worker: Worker
    ):
        return RedirectResponse(url="/jobs")

    async def workers_view(
        self, request: Request, chancy: Chancy, worker: Worker
    ):
        async with chancy.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    sql.SQL(
                        """
                        SELECT
                            *,
                            EXISTS (
                                SELECT 1
                                FROM {leader}
                                WHERE {leader}.worker_id = {workers}.worker_id
                            ) AS is_leader
                        FROM {workers}
                        ORDER BY last_seen DESC
                        """
                    ).format(
                        workers=sql.Identifier(f"{chancy.prefix}workers"),
                        leader=sql.Identifier(f"{chancy.prefix}leader"),
                    )
                )
                workers = await cursor.fetchall()

        return HTMLResponse(
            await self.render("workers.html", request, workers=workers),
        )

    async def queues_view(
        self, request: Request, chancy: Chancy, worker: Worker
    ):
        """
        Returns a list of known queues and their current status.
        """
        async with chancy.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    sql.SQL(
                        """
                        SELECT
                            *,
                            (
                                SELECT COUNT(*)
                                FROM {workers}
                                WHERE {queues}.name = ANY({workers}.queues)
                            ) as worker_count
                        FROM
                            {queues}
                        """
                    ).format(
                        queues=sql.Identifier(f"{chancy.prefix}queues"),
                        workers=sql.Identifier(f"{chancy.prefix}workers"),
                    )
                )
                queues = await cursor.fetchall()

        return HTMLResponse(
            await self.render("queues.html", request, queues=queues),
        )

    async def jobs_view(self, request: Request, chancy: Chancy, worker: Worker):
        """
        Returns a list of known jobs and their current status.
        """
        state = request.query_params.get("state", "pending")
        limit = min(int(request.query_params.get("limit", 20)), 200) + 1

        rules = JobRules.State() == state

        async with chancy.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    sql.SQL(
                        """
                        SELECT
                            *
                        FROM
                            {jobs}
                        WHERE
                            ({rule})
                        ORDER BY
                            {ordering}
                        LIMIT %(limit)s
                        """
                    ).format(
                        jobs=sql.Identifier(f"{chancy.prefix}jobs"),
                        rule=rules.to_sql(),
                        ordering={
                            "running": sql.SQL("started_at ASC, id"),
                            "failed": sql.SQL("completed_at DESC, id"),
                            "succeeded": sql.SQL("completed_at DESC, id"),
                        }.get(state, sql.SQL("created_at DESC, id")),
                    ),
                    {
                        "state": state,
                        # We fetch one more than the limit to see if there are
                        # more jobs available for pagination.
                        "limit": limit + 1,
                    },
                )
                jobs = await cursor.fetchall()

        return HTMLResponse(
            await self.render(
                "jobs.html",
                request,
                jobs=jobs,
                state=state,
                states=[
                    ("Pending", "pending"),
                    ("Running", "running"),
                    ("Failed", "failed"),
                    ("Retrying", "retrying"),
                    ("Succeeded", "succeeded"),
                ],
                limit=limit,
            ),
        )

    async def job_view(self, request: Request, chancy: Chancy, worker: Worker):
        """
        Returns a detailed view of a single job.
        """
        job_id = request.path_params["job_id"]

        async with chancy.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    sql.SQL(
                        """
                        SELECT
                            *
                        FROM
                            {jobs}
                        WHERE
                            id = %(job_id)s
                        """
                    ).format(jobs=sql.Identifier(f"{chancy.prefix}jobs")),
                    {"job_id": job_id},
                )
                job = await cursor.fetchone()

        if job is None:
            return HTMLResponse("Job not found", status_code=404)

        return HTMLResponse(
            await self.render("job.html", request, job=JobInstance.unpack(job)),
        )
