import code
import asyncio
from functools import wraps
from typing import Callable

import click

from chancy import Chancy, Worker, Job, Limit, Reference, Queue, JobInstance
from chancy.utils import import_string


def run_async_command(f):
    """
    Decorator to run a coroutine as a Click command.
    """

    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


@click.group()
@click.option(
    "--app",
    "-a",
    help="The import path for the Chancy app.",
    required=True,
)
@click.pass_context
def cli(ctx: click.Context, app: str):
    """
    Command line interface for Chancy.
    """
    ctx.ensure_object(dict)

    # Might be a Chancy instance or a function we need to call to get one.
    chancy_app: Chancy | Callable[[], Chancy] = import_string(app)

    if isinstance(chancy_app, Chancy):
        ctx.obj["app"] = chancy_app
    else:
        ctx.obj["app"] = chancy_app()


@cli.command("worker")
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
        await Worker(
            chancy,
            worker_id=worker_id,
            tags=set(tags) if tags else None,
        ).start()


@cli.command()
@click.option(
    "--to-version",
    "-t",
    help="The version to migrate to.",
    type=int,
)
@click.pass_context
@run_async_command
async def migrate(ctx: click.Context, to_version: int | None):
    """
    Migrate the database's core tables and any configured plugins up or down
    to the desired version.

    By default, migrates to the latest version, but can be controlled with
    the `--to-version` option.
    """
    chancy: Chancy = ctx.obj["app"]

    async with chancy:
        await chancy.migrate(to_version=to_version)


@cli.command()
@click.pass_context
@run_async_command
async def shell(ctx):
    """
    Start an interactive shell with the Chancy app instance and common
    imports already available.
    """
    chancy: Chancy = ctx.obj["app"]

    async with chancy:
        code.interact(
            local={
                "chancy": chancy,
                "Worker": Worker,
                "Job": Job,
                "JobInstance": JobInstance,
                "Limit": Limit,
                "Reference": Reference,
                "Queue": Queue,
                "asyncio": asyncio,
            }
        )


def main():
    cli()


if __name__ == "__main__":
    main()
