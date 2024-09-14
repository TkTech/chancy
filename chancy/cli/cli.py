from typing import Callable

import click

from chancy import Chancy
from chancy.utils import import_string


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


def main():
    from chancy.cli import misc, queue, worker

    cli.add_command(misc.misc_group)
    cli.add_command(queue.queue_group)
    cli.add_command(worker.worker_group)

    cli()


if __name__ == "__main__":
    main()
