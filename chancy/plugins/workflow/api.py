from dataclasses import asdict

from psycopg import sql
from psycopg.rows import dict_row
from starlette.responses import Response

from chancy.plugins.api import ApiPlugin
from chancy.plugins.workflow import WorkflowPlugin
from chancy.utils import json_dumps


class WorkflowApiPlugin(ApiPlugin):
    def name(self):
        return "workflow"

    def routes(self):
        return [
            {
                "path": "/api/v1/workflows",
                "endpoint": self.get_workflows,
                "methods": ["GET"],
                "name": "get_workflows",
            },
            {
                "path": "/api/v1/workflows/{id}",
                "endpoint": self.get_workflow,
                "methods": ["GET"],
                "name": "get_workflow",
            },
        ]

    @staticmethod
    async def get_workflows(request, *, chancy, worker):
        """
        Get all known workflows.
        """
        async with chancy.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    sql.SQL(
                        """
                        SELECT
                            w.*
                        FROM
                            {workflows_table} w
                        ORDER BY
                            created_at DESC;
                        """
                    ).format(
                        workflows_table=sql.Identifier(
                            f"{chancy.prefix}workflows"
                        ),
                    )
                )

                results = await cursor.fetchall()

                return Response(
                    json_dumps(results),
                    media_type="application/json",
                )

    @staticmethod
    async def get_workflow(request, *, chancy, worker):
        """
        Get a single workflow by ID.
        """
        workflow_id = request.path_params["id"]
        workflow = await WorkflowPlugin.fetch_workflow(chancy, workflow_id)

        return Response(
            json_dumps(asdict(workflow)),
            media_type="application/json",
        )
