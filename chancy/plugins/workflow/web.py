from starlette.responses import Response

from chancy import Chancy, Worker
from chancy.plugins.web import WebPlugin
from chancy.plugins.workflow import WorkflowPlugin, Workflow


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
        ordering = {
            Workflow.State.PENDING: 3,
            Workflow.State.RUNNING: 2,
            Workflow.State.FAILED: 1,
            Workflow.State.COMPLETED: 0,
        }

        wp = WorkflowPlugin()
        workflows = await wp.fetch_workflows(chancy)
        workflows.sort(
            key=lambda w: (ordering[w.state], w.updated_at), reverse=True
        )

        return Response(
            await self.render("workflows.html", request, workflows=workflows),
            media_type="text/html",
        )

    async def workflow_view(self, request, chancy: Chancy, worker: Worker):
        workflow_id = request.path_params["id"]

        wp = WorkflowPlugin()
        workflow = await wp.fetch_workflow(chancy, workflow_id)

        return Response(
            await self.render("workflow.html", request, workflow=workflow),
            media_type="text/html",
        )
