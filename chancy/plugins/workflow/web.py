from io import StringIO
from psycopg import sql
from psycopg.rows import dict_row
from starlette.responses import Response

from chancy import Chancy, Worker, Job
from chancy.plugins.web import WebPlugin
from chancy.plugins.workflow import WorkflowPlugin


class WorkflowWebPlugin(WebPlugin):
    def name(self):
        return "workflow"

    def links(self) -> list[tuple[str, str]]:
        return [("Workflows", "/workflows")]

    def routes(self):
        return [
            {
                "path": "/workflows",
                "endpoint": self.workflows_view,
                "methods": ["GET"],
                "name": "workflows",
            },
            {
                "path": "/workflow/{id}",
                "endpoint": self.workflow_view,
                "methods": ["GET"],
                "name": "workflow",
            },
        ]

    async def workflows_view(self, request, chancy: Chancy, worker: Worker):
        async with chancy.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    sql.SQL(
                        """
                        SELECT
                            w.id,
                            w.name,
                            w.state,
                            w.created_at,
                            w.updated_at,
                            COUNT(
                                CASE
                                    WHEN s.state = 'pending' THEN 1
                                    WHEN s.state = 'running' THEN 1
                                END
                            ) AS pending_steps,
                            COUNT(s.id) AS total_steps
                        FROM {table} w
                        LEFT JOIN {steps} s ON w.id = s.workflow_id
                        GROUP BY w.id
                        ORDER BY w.state, w.created_at DESC
                        """
                    ).format(
                        table=sql.Identifier(f"{chancy.prefix}workflows"),
                        steps=sql.Identifier(f"{chancy.prefix}workflow_steps"),
                    )
                )
                workflows = await cursor.fetchall()

        return Response(
            await self.render("workflows.html", request, workflows=workflows),
            media_type="text/html",
        )

    async def workflow_view(self, request, chancy: Chancy, worker: Worker):
        id = request.path_params["id"]

        wp = WorkflowPlugin()
        workflow = await wp.fetch_workflow(chancy, id)

        with StringIO() as dot:
            wp.generate_dot(workflow, dot)
            dot.seek(0)
            dot = dot.read()

        return Response(
            await self.render(
                "workflow.html", request, workflow=workflow, dot=dot
            ),
            media_type="text/html",
        )
