from pathlib import Path
from functools import partial

import uvicorn
from starlette.applications import Starlette
from starlette.routing import Route, Mount, WebSocketRoute
from starlette.responses import JSONResponse, FileResponse, Response
from starlette.requests import Request
from starlette.staticfiles import StaticFiles
from starlette.websockets import WebSocket
from psycopg import sql
from psycopg.rows import dict_row
from psycopg import AsyncConnection

from chancy import Worker, Chancy
from chancy.plugin import Plugin, PluginScope
from chancy.logger import logger
from chancy.plugins.rule import State
from chancy.utils import json_dumps


class Web(Plugin):
    """
    A plugin that starts a web server for monitoring and debugging of a
    Chancy cluster.

    The web interface can run on every worker, or you may want to run it on a
    dedicated worker where the Web plugin is the only plugin enabled.

    .. warning::

        The web interface is not secure and should not be exposed to the
        public internet. It is intended for use in a secure environment, such
        as a private network or a VPN where only trusted users have access.

    The UI is a single-page application that uses the REST API to communicate
    with the Chancy cluster, build with Parcel, Bulma and React.

    :param port: The port to listen on.
    :param host: The host to listen on.
    :param debug: Whether to run the server in debug mode.
    """

    @classmethod
    def get_scope(cls) -> PluginScope:
        return PluginScope.WORKER

    def __init__(
        self, *, port: int = 8000, host: str = "127.0.0.1", debug: bool = False
    ):
        super().__init__()
        self.port = port
        self.host = host
        self.debug = debug
        self.root = Path(__file__).parent

    async def run(self, worker: Worker, chancy: Chancy):
        """
        Start the web server.
        """

        def _r(f):
            return partial(f, chancy=chancy, worker=worker)

        app = Starlette(
            debug=self.debug,
            routes=[
                Route("/", self.index_view),
                Mount(
                    "/static",
                    app=StaticFiles(directory=self.root / "static"),
                    name="static",
                ),
                Mount(
                    "/api",
                    routes=[
                        Route("/workers", _r(self.workers_view)),
                        Route("/states", _r(self.states_view)),
                        Route("/jobs", _r(self.jobs_view)),
                        Route("/jobs/{job_id}", _r(self.job_view)),
                    ],
                ),
                WebSocketRoute("/ws-gossip", _r(self.ws_gossip_view)),
                # Route all static files
                # We're a single-page app, so we need to catch all routes and
                # return the index.html file, which will do its own routing.
                Route("/{path:path}", self.index_view),
            ],
        )

        config = uvicorn.Config(
            app=app,
            host=self.host,
            port=self.port,
            log_level=logger.level,
        )
        server = uvicorn.Server(config=config)
        await server.serve()

    async def index_view(self, request):
        return FileResponse(self.root / "static" / "index.html")

    @classmethod
    async def workers_view(
        cls, request: Request, chancy: Chancy, worker: Worker
    ):
        """
        Returns a list of known workers and when they were last seen.
        """
        async with chancy.pool.connection() as conn:
            conn: AsyncConnection
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    sql.SQL("SELECT * FROM {workers}").format(
                        workers=sql.Identifier(f"{chancy.prefix}workers")
                    )
                )
                workers = await cursor.fetchall()
                return JSONResponse(
                    [
                        {
                            "worker_id": worker["worker_id"],
                            "last_seen": worker["last_seen"].isoformat(),
                        }
                        for worker in workers
                    ]
                )

    @classmethod
    async def jobs_view(cls, request: Request, chancy: Chancy, worker: Worker):
        """
        Returns a list of known jobs and their current status.
        """
        state = request.query_params.get("state", "running")
        limit = min(int(request.query_params.get("limit", 20)), 200)

        rules = State() == state

        async with chancy.pool.connection() as conn:
            conn: AsyncConnection
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
                            "running": sql.SQL("started_at ASC"),
                            "failed": sql.SQL("completed_at DESC"),
                            "succeeded": sql.SQL("completed_at DESC"),
                        }.get(state, sql.SQL("created_at DESC")),
                    ),
                    {
                        "state": state,
                        # We fetch one more than the limit to see if there are
                        # more jobs available for pagination.
                        "limit": limit + 1,
                    },
                )
                jobs = await cursor.fetchall()
                return Response(json_dumps(jobs), media_type="application/json")

    @classmethod
    async def job_view(cls, request: Request, chancy: Chancy, worker: Worker):
        """
        Returns a single job and its current status.
        """
        job_id = request.path_params["job_id"]
        async with chancy.pool.connection() as conn:
            conn: AsyncConnection
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
                return Response(json_dumps(job), media_type="application/json")

    @classmethod
    async def states_view(
        cls, request: Request, chancy: Chancy, worker: Worker
    ):
        """
        Returns a list of known states.
        """
        return JSONResponse(
            [
                {"name": "pending", "count": 0},
                {"name": "running", "count": 0},
                {"name": "retrying", "count": 0},
                {"name": "failed", "count": 0},
                {"name": "succeeded", "count": 0},
            ]
        )

    @classmethod
    async def ws_gossip_view(
        cls, websocket: WebSocket, chancy: Chancy, worker: Worker
    ):
        """
        Websocket endpoint for listening into all cluster gossip.
        """
        await websocket.accept()

        async def callback(event, body):
            await websocket.send_text(
                json_dumps({"event": event, "body": body})
            )

        try:
            worker.hub.on("*", callback)
            async for _ in websocket.iter_text():
                # We currently don't _do_ anything with incoming messages.
                pass
        finally:
            worker.hub.off("*", callback)
            await websocket.close()
