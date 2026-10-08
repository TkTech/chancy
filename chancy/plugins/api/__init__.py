__all__ = ("Api", "AuthBackend", "SimpleAuthBackend")
import os
import secrets
from functools import partial
from pathlib import Path
from urllib.parse import quote

import uvicorn
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import HTMLResponse, Response
from starlette.staticfiles import StaticFiles
from starlette.types import Scope

from chancy import Chancy, Worker
from chancy.plugin import Plugin
from chancy.plugins.api.auth import (
    AuthBackend,
    SimpleAuthBackend,
    TokenAuthBackend,
)
from chancy.plugins.api.core import CoreApiPlugin
from chancy.plugins.api.plugin import ApiPlugin
from chancy.utils import import_string


class _SPAStaticFiles(StaticFiles):
    """
    A StaticFiles class that serves the SPA index.html file for any path that
    doesn't match an existing file.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._index_path, _ = super().lookup_path("index.html")
        self._index_html = Path(self._index_path).read_text(encoding="utf-8")

    def lookup_path(self, path: str) -> tuple[str, os.stat_result | None]:
        full_path, stat_result = super().lookup_path(path)
        if stat_result is None:
            return super().lookup_path("./index.html")
        return full_path, stat_result

    def file_response(
        self,
        full_path: str | os.PathLike,
        stat_result: os.stat_result,
        scope: Scope,
        status_code: int = 200,
    ) -> Response:
        if full_path != self._index_path:
            return super().file_response(
                full_path, stat_result, scope, status_code
            )

        # The outer ASGI mounts (and any configured proxy root_path) determine
        # the public URL. Set the base before the browser loads any assets.
        root_path = "/" + scope.get("root_path", "").strip("/")
        base_href = quote(root_path.rstrip("/") + "/", safe="/")
        response = HTMLResponse(
            self._index_html.replace(
                '<base href="/">', f'<base href="{base_href}">', 1
            ),
            status_code=status_code,
            # The HTML varies by mount and must not reuse static-file ETags.
            headers={"Cache-Control": "no-store"},
        )
        if scope["method"] == "HEAD":
            response.body = b""
        return response


class Api(Plugin):
    """
    Provides an API and a dashboard for viewing the state of the Chancy cluster.

    Running this plugin requires a few additional dependencies, you can install
    them with:

    .. code-block:: bash

        pip install chancy[web]

    To have the API run permanently on each Worker, you can add it to the
    plugins list when creating the Chancy instance:

    .. code-block:: python

        from chancy.plugins.api import Api
        from chancy.plugins.api.auth import SimpleAuthBackend

        async with Chancy(..., plugins=[
            Api(
                authentication_backend=SimpleAuthBackend({"admin": "password"}),
                secret_key="<a strong, random secret key>",
            ),
        ]) as chancy:
            ...

    .. note::

        The API is still mostly undocumented, as its development is driven by
        the needs of the dashboard and may change significantly before it
        becomes stable.

    Since it's very common to only want the dashboard temporarily, you can
    start it with the CLI. This can also be used to connect to a remote Chancy
    instance:

    .. code-block:: bash

        pip install chancy[cli,web]
        chancy --app worker.chancy worker web

    This will run the API and dashboard on port 8000 by default (you can change
    this with the ``--port`` and ``--host`` flags), and generate a temporary
    random password for the admin user.

    Screenshots
    -----------

    .. image:: ../misc/ux_jobs.png
        :alt: Jobs page

    .. image:: ../misc/ux_job_failed.png
        :alt: Failed job page

    .. image:: ../misc/ux_queue.png
        :alt: Queue page

    .. image:: ../misc/ux_workflow.png
        :alt: Worker page


    :param port: The port to listen on.
    :param host: The host to listen on.
    :param debug: Whether to run the server in debug mode.
    :param allow_origins: A list of origins that are allowed to access the API.
    """

    def __init__(
        self,
        *,
        port: int = 8000,
        host: str = "127.0.0.1",
        debug: bool = False,
        allow_origins: list[str] | None = None,
        secret_key: str | None = None,
        authentication_backend: AuthBackend,
    ):
        super().__init__()
        self.port = port
        self.host = host
        self.debug = debug
        self.allow_origins = allow_origins or []
        self.plugins: set[type[ApiPlugin]] = {CoreApiPlugin}
        self.authentication_backend = authentication_backend
        self.secret_key = secret_key or secrets.token_urlsafe(32)

    @staticmethod
    def get_identifier() -> str:
        return "chancy.api"

    def build_starlette_app(self, worker: Worker, chancy: Chancy) -> Starlette:
        """
        Build an ASGI app containing the API and dashboard.
        """
        plugins = self.plugins.copy()
        for plug in chancy.plugins.values():
            api_plugin = plug.api_plugin()
            if api_plugin is None:
                continue

            api_plugin = import_string(api_plugin)
            if not issubclass(api_plugin, ApiPlugin):
                continue

            plugins.add(api_plugin)

        def _r(f):
            return partial(f, chancy=chancy, worker=worker)

        # Use pure token-based authentication for API requests.
        backend = TokenAuthBackend(self.secret_key)

        app = Starlette(
            debug=self.debug,
            middleware=[
                Middleware(
                    CORSMiddleware,
                    allow_origins=self.allow_origins,
                    allow_credentials=False,
                    allow_headers=["*"],
                    allow_methods=["*"],
                ),
                # Session middleware removed in token-only mode.
                Middleware(
                    AuthenticationMiddleware,
                    backend=backend,
                ),
            ],
        )

        # Look through all the enabled plugins for any that implement the
        # ApiPlugin interface. If they do, we merge them into our Starlette
        # app.
        for api_plugin in plugins:
            wp = api_plugin(self)
            chancy.log.info(f"Loading API sub-plugin {wp.name()}")

            for route in wp.routes():
                if route.get("is_websocket"):
                    app.add_websocket_route(
                        route["path"],
                        _r(route["endpoint"]),
                        name=route["name"],
                    )
                else:
                    app.add_route(
                        route["path"],
                        _r(route["endpoint"]),
                        methods=route["methods"],
                        name=route["name"],
                    )

        # Add the wildcard route to the end of the list so that it doesn't
        # override any other routes and serves anything that should be handled
        # by the UI SPA.
        app.mount(
            "/",
            _SPAStaticFiles(
                packages=[("chancy.plugins.api", "dist")],
                html=True,
            ),
            name="ui",
        )

        return app

    async def run(self, worker: Worker, chancy: Chancy):
        """
        Start the standalone web server.
        """
        app = self.build_starlette_app(worker, chancy)

        server = uvicorn.Server(
            config=uvicorn.Config(
                app=app,
                host=self.host,
                port=self.port,
                log_level="error",
            )
        )

        chancy.log.info(f"Dashboard running at http://{self.host}:{self.port}")

        await server.serve()
