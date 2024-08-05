import inspect
from pathlib import Path
from functools import partial
import importlib.resources
from typing import Type

import jinja2
import jinja2.loaders
import uvicorn
from starlette.applications import Starlette
from starlette.staticfiles import StaticFiles

from chancy import Worker, Chancy
from chancy.plugin import Plugin, PluginScope
from chancy.plugins.web.base import BaseWebPlugin
from chancy.plugins.web.filters import time_until, relative_time
from chancy.plugins.web.plugin import WebPlugin


class Web(Plugin):
    """
    A plugin that provides a simple dashboard for monitoring the state of
    the Chancy cluster.

    Running this plugin requires a few additional dependencies, you can install
    them with:

    .. code-block:: bash

        pip install chancy[web]

    Most built-in Chancy plugins provide a WebPlugin implementation that
    adds additional views to the web interface. You can also create your
    own plugins that implement the WebPlugin interface to add custom views.

    .. warning::

        The web interface is not secure and should not be exposed to the
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
        plugins: set[Type[WebPlugin]] | None = None,
        port: int = 8000,
        host: str = "127.0.0.1",
        debug: bool = False,
    ):
        super().__init__()
        self.plugins = plugins or {BaseWebPlugin}
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

        app = Starlette(debug=self.debug)

        # We're really abusing the internals of how the PrefixLoader is
        # implemented here. Works perfectly fine, but we should make our
        # own PluginTemplateLoader that does this properly.
        web_plugins = []
        mapping = {}
        environment = jinja2.Environment(
            loader=jinja2.PrefixLoader(mapping),
            autoescape=jinja2.select_autoescape(),
            enable_async=True,
        )
        environment.globals.update(
            {
                "plugins": web_plugins,
                "worker": worker,
                "chancy": chancy,
            }
        )
        environment.filters["time_until"] = time_until
        environment.filters["relative_time"] = relative_time

        # Look through all the enabled plugins for any that implement the
        # WebPlugin interface. If they do, we merge them into our Starlette
        # app.
        for wp in self.plugins:
            wp = wp(environment)
            web_plugins.append(wp)

            package = inspect.getmodule(wp).__package__

            chancy.log.info(f"Loading web plugin {wp.name()} from {package}")

            mapping[wp.name()] = jinja2.loaders.PackageLoader(
                package, "templates"
            )

            for route in wp.routes():
                app.add_route(
                    route["path"],
                    _r(route["endpoint"]),
                    methods=route["methods"],
                    name=route["name"],
                )

            has_static = (
                importlib.resources.files(package).joinpath("static").is_dir()
            )

            if has_static:
                chancy.log.debug(
                    f"Adding static files for {wp.name()} from {package}"
                )
                app.mount(
                    f"/static/{wp.name()}",
                    StaticFiles(packages=[(package, "static")]),
                    name=f"{wp.name()}_static",
                )

        server = uvicorn.Server(
            config=uvicorn.Config(
                app=app,
                host=self.host,
                port=self.port,
                log_level="error",
            )
        )
        await server.serve()
