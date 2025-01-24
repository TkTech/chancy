import asyncio
import code

import click
from psycopg import AsyncCursor

from chancy import Chancy, Worker, Job, QueuedJob, Limit, Reference, Queue
from chancy.cli import run_async_command
from chancy.migrate import Migrator


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


async def _check_migrations(migrator: Migrator, cursor: AsyncCursor):
    """
    Check the migrations for a migrator.
    """
    good = "✓"
    bad = "✗"

    all_migrations = migrator.discover_all_migrations()
    current_version = await migrator.get_current_version(cursor)

    for i, migration in enumerate(all_migrations, 1):
        is_applied = i <= current_version
        click.echo(
            f"| |- [{good if is_applied else bad}] {migration.__class__.__name__} "
        )


@misc_group.command()
@click.pass_context
@run_async_command
async def check_migrations(ctx: click.Context):
    """
    Check the current migration status of the database.
    """
    chancy: Chancy = ctx.obj["app"]

    async with chancy:
        migrator = Migrator("chancy", "chancy.migrations", prefix=chancy.prefix)
        async with chancy.pool.connection() as conn:
            async with conn.cursor() as cursor:
                click.echo("Chancy Core")
                await _check_migrations(migrator, cursor)

                for plugin in chancy.plugins:
                    migrator = plugin.migrator(chancy)
                    if migrator is None:
                        continue

                    click.echo(f"|-{plugin.__class__.__name__} Plugin")
                    await _check_migrations(migrator, cursor)


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
