import os
from pathlib import Path
from functools import partial
from typing import Type

import uvicorn
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.staticfiles import StaticFiles

from chancy import Worker, Chancy
from chancy.plugin import Plugin, PluginScope
from chancy.plugins.api.core import CoreApiPlugin
from chancy.plugins.api.plugin import ApiPlugin


class _SPAStaticFiles(StaticFiles):
    """
    A StaticFiles class that serves the SPA index.html file for any path that
    doesn't match an existing file.
    """

    def lookup_path(self, path: str) -> tuple[str, os.stat_result | None]:
        full_path, stat_result = super().lookup_path(path)
        if stat_result is None:
            return super().lookup_path("./index.html")
        return full_path, stat_result


class Api(Plugin):
    """
    Provides an API and a dashboard for viewing the state of the Chancy cluster.

    Running this plugin requires a few additional dependencies, you can install
    them with:

    .. code-block:: bash

        pip install chancy[web]

    Most built-in Chancy plugins provide an ApiPlugin implementation that
    adds additional endpoints. Enable them by passing them to the plugin
    constructor:

    .. code-block:: python

        from chancy.plugins.api import Api
        from chancy.plugins.api.core import CoreApiPlugin
        from chancy.plugins.workflow import WorkflowPlugin
        from chancy.plugins.workflow.api import WorkflowApiPlugin

        async with Chancy(..., plugins=[
            WorkflowPlugin(),
            Api(plugins={CoreApiPlugin, WorkflowApiPlugin}),
        ]) as chancy:
            ...

    .. note::

        The API is still mostly undocumented, as its development is driven by
        the needs of the dashboard and may change significantly before it
        becomes stable.

    .. warning::

        The api interface is not secure and should not be exposed to the
        public internet. It is intended for use in a secure environment, such
        as a private network or a VPN where only trusted users have access.

    :param plugins: A set of plugins that provide additional API endpoints.
    :param port: The port to listen on.
    :param host: The host to listen on.
    :param debug: Whether to run the server in debug mode.
    :param allow_origins: A list of origins that are allowed to access the API.
    """

    @classmethod
    def get_scope(cls) -> PluginScope:
        return PluginScope.WORKER

    def __init__(
        self,
        *,
        plugins: set[Type[ApiPlugin]] | None = None,
        port: int = 8000,
        host: str = "127.0.0.1",
        debug: bool = False,
        allow_origins: list[str] | None = None,
    ):
        super().__init__()
        self.plugins = plugins or {CoreApiPlugin}
        self.port = port
        self.host = host
        self.debug = debug
        self.root = Path(__file__).parent
        self.allow_origins = allow_origins or []

    async def run(self, worker: Worker, chancy: Chancy):
        """
        Start the web server.
        """

        def _r(f):
            return partial(f, chancy=chancy, worker=worker)

        app = Starlette(
            debug=self.debug,
            middleware=[
                Middleware(
                    CORSMiddleware,
                    allow_origins=self.allow_origins,
                )
            ],
        )

        web_plugins = []
        # Look through all the enabled plugins for any that implement the
        # ApiPlugin interface. If they do, we merge them into our Starlette
        # app.
        for api_plugin in self.plugins:
            wp = api_plugin()
            web_plugins.append(wp)
            chancy.log.info(f"Loading API sub-plugin {wp.name()}")

            for route in wp.routes():
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
