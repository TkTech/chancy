import asyncio
import code

import click

from chancy import Chancy, Worker, Job, QueuedJob, Limit, Reference, Queue
from chancy.cli import run_async_command


@click.group(name="misc")
def misc_group():
    """
    Miscellaneous commands.
    """
    pass


@misc_group.command()
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


@misc_group.command()
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
                "QueuedJob": QueuedJob,
                "Limit": Limit,
                "Reference": Reference,
                "Queue": Queue,
                "asyncio": asyncio,
            }
        )
