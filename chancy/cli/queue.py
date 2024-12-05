import json

import click
from click import Context

from chancy import Chancy, Job, Queue
from chancy.cli import run_async_command


@click.group(name="queue")
def queue_group():
    """
    Queue management commands.
    """
    pass


@queue_group.command()
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
    default=1,
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
    Push a job.

    Note that this method of pushing a job ignores any defaults that might be
    defined on the job using the @job() decorator, such as the queue name.
    While this can be an inconvenience, it allows pushing jobs without having
    to import any project-specific code.
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
