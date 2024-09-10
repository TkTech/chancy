import code
import asyncio
import json
from email.policy import default
from enum import unique
from functools import wraps
from typing import Callable

import click
from click import Context

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


@cli.command()
@click.argument("job")
@click.option(
    "--queue",
    "-q",
    help="The queue to push the job to.",
    default="default",
)
@click.option(
    "--priority",
    "-p",
    help="The job's priority.",
    default=0,
    type=int,
)
@click.option(
    "--unique-key",
    "-u",
    help="The job's unique key.",
)
@click.option(
    "--max-attempts",
    "-m",
    help="The maximum number of attempts to make.",
    type=int,
)
@click.option(
    "--kwargs",
    "-k",
    help="A JSON blob containing the keyword arguments to pass to the job.",
)
@click.pass_context
@run_async_command
async def push(
    ctx: Context,
    job: str,
    queue: str,
    priority: int,
    unique_key: str,
    max_attempts: int,
    kwargs: str | None = None,
):
    """
    Push a job to the default queue.
    """
    chancy: Chancy = ctx.obj["app"]

    if kwargs is not None:
        kwargs = json.loads(kwargs)

    async with chancy:
        await chancy.push(
            Job(
                func=job,
                queue=queue,
                priority=priority,
                unique_key=unique_key,
                max_attempts=max_attempts,
                kwargs=kwargs,
            )
        )


@click.group("queue")
@click.pass_context
def queue_group(ctx: click.Context):
    """
    Queue management commands.
    """
    pass


@queue_group.command("list")
@click.pass_context
@run_async_command
async def list_queues(ctx: click.Context):
    """
    List all the queues.
    """
    chancy: Chancy = ctx.obj["app"]

    async with chancy:
        queues = await chancy.get_all_queues()

        for queue in queues:
            click.echo(f"{queue!r}")


@queue_group.command("declare")
@click.argument("name")
@click.option(
    "--concurrency",
    "-c",
    default=1,
    type=int,
    help="The number of simultaneous jobs to process.",
)
@click.option(
    "--executor",
    "-e",
    default="chancy.executors.process.ProcessExecutor",
    help="The executor to use.",
)
@click.option(
    "--rate-limit", "-r", type=int, help="The global queue rate limit."
)
@click.option(
    "--rate-limit-window",
    "-w",
    type=int,
    help="The global queue rate limit window.",
)
@click.option(
    "--polling-interval",
    "-p",
    help="The interval to poll the queue for new jobs.",
    type=int,
    default=5,
)
@click.option(
    "--tags",
    "-t",
    help="Extra tags to apply to the queue.",
    multiple=True,
)
@click.pass_context
@run_async_command
async def declare_queue(
    ctx: click.Context,
    name: str,
    concurrency: int | None,
    executor: str | None,
    rate_limit: int | None,
    rate_limit_window: int | None,
    polling_interval: int | None,
    tags: list[str] | None,
):
    """
    Declare a new queue.
    """
    chancy: Chancy = ctx.obj["app"]

    async with chancy:
        await chancy.declare(
            Queue(
                name,
                concurrency=concurrency,
                executor=executor,
                rate_limit=rate_limit,
                rate_limit_window=rate_limit_window,
                polling_interval=polling_interval,
                tags=set(tags) if tags else {r".*"},
            )
        )


@queue_group.command("delete")
@click.argument("name")
@click.pass_context
@run_async_command
async def delete_queue(ctx: click.Context, name: str):
    """
    Delete a queue.
    """
    chancy: Chancy = ctx.obj["app"]

    if click.confirm(f"Are you sure you want to delete the queue {name}?"):
        async with chancy:
            await chancy.delete_queue(name)


def main():
    cli.add_command(queue_group)
    cli()


if __name__ == "__main__":
    main()
