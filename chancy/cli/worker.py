import secrets
from contextlib import AsyncExitStack

import click
from click.core import ParameterSource

from chancy import Chancy, Worker
from chancy.cli import run_async_command
from chancy.errors import MigrationsNeededError
from chancy.session import PluginSession


@click.group(name="worker")
def worker_group():
    """
    Worker management commands.
    """


@worker_group.command("start")
@click.option("--worker-id", "-w", help="The worker ID to use.")
@click.option(
    "--tags",
    "-t",
    help="Extra tags to apply to the worker.",
    multiple=True,
)
@click.pass_context
@run_async_command
async def worker_command(
    ctx: click.Context, worker_id: str | None, tags: list[str] | None
):
    """
    Start a worker.
    """
    chancy: Chancy = ctx.obj["app"]

    async with chancy:
        try:
            async with Worker(
                chancy,
                worker_id=worker_id,
                tags=set(tags) if tags else None,
            ) as worker:
                await worker.wait_for_shutdown()
        except MigrationsNeededError:
            click.echo(
                "The database is not up to date and is missing migrations.\n"
                "Please run `chancy misc migrate` to update the database.\n"
                "You can check the current migration status with"
                " `chancy misc check-migrations`."
            )
            return 1


@worker_group.command("web")
@click.option("--host", "-h", help="The host to bind to.", default="localhost")
@click.option("--port", "-p", help="The port to bind to.", default=8000)
@click.option(
    "--debug", "-d", help="Run the server in debug mode.", is_flag=True
)
@click.option(
    "--allow-origin",
    "-o",
    help="A list of allowed origins.",
    multiple=True,
    default=lambda: ["*"],
)
@click.pass_context
@run_async_command
async def web_command(
    ctx: click.Context,
    host: str,
    port: int,
    debug: bool,
    allow_origin: list[str],
):
    """
    Start the Chancy dashboard.
    """
    from chancy.plugins.api import Api
    from chancy.plugins.api.auth import SimpleAuthBackend

    chancy: Chancy = ctx.obj["app"]

    async with chancy:
        worker = Worker(chancy, tags=set())

        if not (api := chancy.plugins.get("chancy.api")):
            chancy.log.info(
                "No API plugin was configured on the Chancy application,"
                " falling back to the default API."
            )
            auth = SimpleAuthBackend({"admin": secrets.token_urlsafe(32)})
            api = Api(
                host=host,
                port=port,
                allow_origins=allow_origin,
                debug=debug,
                authentication_backend=auth,
            )
            chancy.log.warning(
                f"No username or password was provided for the API, defaulting"
                f" to 'admin' with a random password: {auth.users['admin']}"
            )
        else:
            # Override API plugin settings only if CLI flags were explicitly provided
            if ctx.get_parameter_source("host") != ParameterSource.DEFAULT:
                api.host = host
            if ctx.get_parameter_source("port") != ParameterSource.DEFAULT:
                api.port = port
            if ctx.get_parameter_source("debug") != ParameterSource.DEFAULT:
                api.debug = debug
            if (
                ctx.get_parameter_source("allow_origin")
                != ParameterSource.DEFAULT
            ):
                api.allow_origins = allow_origin

        async with AsyncExitStack() as resources:
            sessions = []
            for plugin in (chancy.plugins.get("chancy.metrics"), api):
                if plugin is None:
                    continue
                session = PluginSession(plugin, worker)
                resources.push_async_callback(session.close)
                sessions.append(session)
                await plugin.setup(session)

            for session in sessions:
                resources.push_async_callback(
                    session.plugin.on_worker_stopped, worker=worker
                )
            resources.push_async_callback(worker.flush)
            resources.push_async_callback(worker.manager.cancel_all)
            for session in sessions:
                session.start()

            # Run dashboard infrastructure without processing queues.
            if chancy.notifications:
                worker.manager.add(
                    "notifications", worker._maintain_notifications()
                )
            worker.manager.add("updates", worker._maintain_updates())
            await worker.wait_for_shutdown()
