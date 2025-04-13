__all__ = ("Api", "AuthBackend", "SimpleAuthBackend")
import os
import secrets
from pathlib import Path
from functools import partial
from typing import Type

import uvicorn
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.staticfiles import StaticFiles

from chancy import Worker, Chancy
from chancy.plugin import Plugin
from chancy.plugins.api.auth import AuthBackend, SimpleAuthBackend
from chancy.plugins.api.core import CoreApiPlugin
from chancy.plugins.api.plugin import ApiPlugin
from chancy.utils import import_string


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
    You can use this to view the status of jobs, queues, workers, workflows,
    metrics and more.

    Running this plugin requires a few additional dependencies, you can install
    them with:

    .. code-block:: bash

        pip install chancy[cli,web]

    You can then start the API and dashboard with:

    .. code-block:: bash

        chancy --app worker.chancy worker web

    This will run the API and dashboard on port 8000 by default (you can change
    this with the ``--port`` and ``--host`` flags), and generate a temporary
    random password for the admin user.

    To customize the API, pass it as plugin to the Chancy instance:

    .. code-block:: python

        from chancy import Chancy
        from chancy.plugins.api import Api, SimpleAuthBackend

        chancy = Chancy(..., plugins=[
            Api(
                authentication_backend=SimpleAuthBackend({"admin": "password"}),
                secret_key="<a strong, random secret key>",
            ),
        ])

    You can write your own authentication backend by subclassing the
    :class:`~chancy.plugins.api.auth.AuthBackend` class. The default
    :class:`~chancy.plugins.api.auth.SimpleAuthBackend` class uses a simple
    dictionary to store the users and passwords. The password is stored in
    plaintext, so this is not recommended for production use. If you're using
    Django, Chancy includes a Django authentication integration, see the
    :doc:`Django integration documentation </howto/django>` for more
    information.

    .. note::

        The API itself is still mostly undocumented, as its development is
        driven by the needs of the dashboard and may change significantly
        before it becomes stable.

    Screenshots
    -----------

    .. image:: ../misc/ux_login.png
        :alt: Login page

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
    :param allow_credentials: Whether to allow credentials to be sent with
                              requests. This is required for cookies to work.
    :param secret_key: A secret key used to sign cookies. This should be a
                       strong, random key. If not provided, a random key will
                       be generated each time the server is started.
    :param authentication_backend: The authentication backend to use.
    :param autostart: Whether to start the server automatically when the
                      worker starts.
    """

    AUTOSTART = False

    def __init__(
        self,
        *,
        port: int = 8000,
        host: str = "127.0.0.1",
        debug: bool = False,
        allow_origins: list[str] | None = None,
        allow_credentials: bool = False,
        secret_key: str | None = None,
        authentication_backend: AuthBackend,
        autostart: bool = False,
    ):
        super().__init__()
        self.port = port
        self.host = host
        self.debug = debug
        self.root = Path(__file__).parent
        self.allow_origins = allow_origins or []
        self.allow_credentials = allow_credentials
        self.plugins: set[Type[ApiPlugin]] = {CoreApiPlugin}
        self.authentication_backend = authentication_backend
        self.secret_key = secret_key or secrets.token_urlsafe(32)
        self._should_autostart = autostart

    @staticmethod
    def get_identifier() -> str:
        return "chancy.api"

    def should_autostart(self) -> bool:
        return self._should_autostart

    async def run(self, worker: Worker, chancy: Chancy):
        """
        Start the web server.
        """
        for plug in chancy.plugins.values():
            api_plugin = plug.api_plugin()
            if api_plugin is None:
                continue

            api_plugin = import_string(api_plugin)
            if not issubclass(api_plugin, ApiPlugin):
                continue

            self.plugins.add(api_plugin)

        def _r(f):
            return partial(f, chancy=chancy, worker=worker)

        app = Starlette(
            debug=self.debug,
            middleware=[
                Middleware(
                    CORSMiddleware,
                    allow_origins=self.allow_origins,
                    allow_credentials=self.allow_credentials,
                    allow_methods=[
                        "GET",
                        "POST",
                    ],
                ),
                Middleware(SessionMiddleware, secret_key=self.secret_key),
                Middleware(
                    AuthenticationMiddleware,
                    backend=self.authentication_backend,
                ),
            ],
        )

        web_plugins = []
        # Look through all the enabled plugins for any that implement the
        # ApiPlugin interface. If they do, we merge them into our Starlette
        # app.
        for api_plugin in self.plugins:
            wp = api_plugin(self)
            web_plugins.append(wp)
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
