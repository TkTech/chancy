import asyncio
from psycopg import sql
from psycopg.rows import dict_row
from starlette.responses import Response
from starlette.websockets import WebSocket, WebSocketDisconnect

from chancy.hub import Event
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
            {
                "path": "/api/v1/events",
                "endpoint": self.events_websocket,
                "methods": ["GET", "POST", "DELETE"],
                "name": "events_websocket",
                "is_websocket": True,
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
                        plugin.__class__.__name__
                        for plugin in chancy.plugins.values()
                    ]
                }
            ),
            media_type="application/json",
        )

    @staticmethod
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

        rule = JobRules.State() == "pending"

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

    @staticmethod
    async def events_websocket(request, *, chancy, worker):
        """
        A WebSocket endpoint that sends all hub events to connected clients.
        """
        websocket = WebSocket(request.scope, request.receive, request.send)
        await websocket.accept()

        # Queue to store events
        event_queue = asyncio.Queue()

        # Event handler to capture events from the hub
        async def event_handler(event: Event):
            await event_queue.put(
                {
                    "name": event.name,
                    "body": event.body,
                    "timestamp": asyncio.get_event_loop().time(),
                }
            )

        # Register the event handler with the hub
        worker.hub.on_any(event_handler)

        try:
            while True:
                # Wait for events or ping every 30 seconds
                try:
                    event_data = await asyncio.wait_for(
                        event_queue.get(), timeout=30
                    )
                    await websocket.send_text(json_dumps(event_data))
                except asyncio.TimeoutError:
                    # Send a ping to keep the connection alive
                    await websocket.send_text(json_dumps({"type": "ping"}))
        except WebSocketDisconnect:
            # Clean up when the client disconnects
            worker.hub.remove_on_any(event_handler)
        except Exception as e:
            chancy.log.error(f"WebSocket error: {e}")
            worker.hub.remove_on_any(event_handler)
