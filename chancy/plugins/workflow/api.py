from dataclasses import asdict
from uuid import UUID

from psycopg import sql
from psycopg.rows import dict_row
from starlette.authentication import requires
from starlette.responses import Response

from chancy.plugins.api import ApiPlugin
from chancy.plugins.workflow import WorkflowPlugin
from chancy.utils import json_dumps


class WorkflowApiPlugin(ApiPlugin):
    """
    API plugin for workflows.
    """

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
    @requires(["authenticated"])
    async def get_workflows(request, *, chancy, worker):
        """
        List workflows in descending ID order with optional filtering.

        Pass the last returned ID as ``before`` to fetch the next page of
        lower IDs with the same filters. Creation and update times do not
        affect this order. Pages are live results, not a snapshot: workflows
        may be removed or stop matching filters between requests.

        Set ``pagination=true`` to receive an object with ``items``,
        ``has_more``, and ``next_cursor`` (an ID to pass as ``before``, or
        null on the last page). Otherwise the response remains an array.
        ``limit`` must be positive and is capped at 100.
        """
        from chancy.plugins.api.core import parse_filters
        from chancy.rule import Rule

        # Get filter parameters
        filters_param = request.query_params.get("filters")
        try:
            limit = min(int(request.query_params.get("limit", "100")), 100)
            if limit < 1:
                raise ValueError
        except ValueError:
            return Response(
                json_dumps({"title": "limit must be a positive integer"}),
                media_type="application/json",
                status_code=422,
            )
        before = request.query_params.get("before")
        try:
            before = UUID(before) if before else None
        except ValueError:
            return Response(
                json_dumps({"title": "before must be a valid UUID"}),
                media_type="application/json",
                status_code=422,
            )
        pagination = request.query_params.get("pagination") == "true"
        fetch_limit = limit + 1 if pagination else limit

        # Process filter triples using helper
        field_config = {
            "state": (Rule("state"), ["="]),
            "name": (Rule("name"), ["=", "~"]),
        }

        rule, error = parse_filters(filters_param, field_config)
        if error:
            return error

        async with (
            chancy.pool.connection() as conn,
            conn.cursor(row_factory=dict_row) as cursor,
        ):
            conditions = [rule.to_sql()] if rule else []
            if before is not None:
                conditions.append(
                    sql.SQL("w.id < {}").format(sql.Literal(before))
                )
            query = sql.SQL(
                """
                    SELECT
                        w.id,
                        w.name,
                        w.state,
                        w.created_at,
                        w.updated_at,
                        COALESCE(stats.pending_steps, 0) as pending_steps,
                        COALESCE(stats.running_steps, 0) as running_steps,
                        COALESCE(stats.succeeded_steps, 0) as succeeded_steps,
                        COALESCE(stats.failed_steps, 0) as failed_steps,
                        COALESCE(stats.retrying_steps, 0) as retrying_steps
                    FROM {workflows_table} w
                    LEFT JOIN (
                        SELECT
                            ws.workflow_id,
                            COUNT(CASE WHEN j.state = 'pending' THEN 1 END) as pending_steps,
                            COUNT(CASE WHEN j.state = 'running' THEN 1 END) as running_steps,
                            COUNT(CASE WHEN j.state = 'succeeded' THEN 1 END) as succeeded_steps,
                            COUNT(CASE WHEN j.state = 'failed' THEN 1 END) as failed_steps,
                            COUNT(CASE WHEN j.state = 'retrying' THEN 1 END) as retrying_steps
                        FROM {workflow_steps_table} ws
                        LEFT JOIN {jobs_table} j ON ws.job_id = j.id
                        GROUP BY ws.workflow_id
                    ) stats ON w.id = stats.workflow_id
                    WHERE {conditions}
                    ORDER BY w.id DESC
                    LIMIT {limit}
                """
            ).format(
                workflows_table=sql.Identifier(f"{chancy.prefix}workflows"),
                workflow_steps_table=sql.Identifier(
                    f"{chancy.prefix}workflow_steps"
                ),
                jobs_table=sql.Identifier(f"{chancy.prefix}jobs"),
                conditions=sql.SQL(" AND ").join(conditions)
                if conditions
                else sql.SQL("TRUE"),
                limit=sql.Literal(fetch_limit),
            )

            await cursor.execute(query)
            results = await cursor.fetchall()

            if pagination:
                has_more = len(results) > limit
                return Response(
                    json_dumps(
                        {
                            "items": results[:limit],
                            "has_more": has_more,
                            "next_cursor": str(results[limit - 1]["id"])
                            if has_more
                            else None,
                        }
                    ),
                    media_type="application/json",
                )
            return Response(
                json_dumps(results),
                media_type="application/json",
            )

    @staticmethod
    @requires(["authenticated"])
    async def get_workflow(request, *, chancy, worker):
        """
        Get a single workflow by ID.
        """
        workflow_id = request.path_params["id"]
        workflow = await WorkflowPlugin.fetch_workflow(chancy, workflow_id)

        if workflow is None:
            return Response(
                json_dumps({"error": "Workflow not found"}),
                media_type="application/json",
                status_code=404,
            )

        return Response(
            json_dumps(asdict(workflow)),
            media_type="application/json",
        )
