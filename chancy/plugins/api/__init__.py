from pathlib import Path
from functools import partial
from typing import Type

import uvicorn
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware

from chancy import Worker, Chancy
from chancy.plugin import Plugin, PluginScope
from chancy.plugins.api.core import CoreApiPlugin
from chancy.plugins.api.plugin import ApiPlugin


class Api(Plugin):
    """
    A plugin that provides a Starlette and Uvicorn-based HTTP API for
    querying the state of the worker and the overall cluster.

    Running this plugin requires a few additional dependencies, you can install
    them with:

    .. code-block:: bash

        pip install chancy[web]

    Most built-in Chancy plugins provide an ApiPlugin implementation that
    adds additional endpoints.

    .. warning::

        The api interface is not secure and should not be exposed to the
        public internet. It is intended for use in a secure environment, such
        as a private network or a VPN where only trusted users have access.

    :param port: The port to listen on.
    :param host: The host to listen on.
    :param debug: Whether to run the server in debug mode.
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

        server = uvicorn.Server(
            config=uvicorn.Config(
                app=app,
                host=self.host,
                port=self.port,
                log_level="error",
            )
        )

        chancy.log.info(f"API running at http://{self.host}:{self.port}")

        await server.serve()
