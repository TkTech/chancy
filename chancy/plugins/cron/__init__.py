import json
from datetime import datetime, timezone

from psycopg import sql
from croniter import croniter

from chancy.plugin import Plugin, PluginScope
from chancy.worker import Worker
from chancy.app import Chancy
from chancy import Job


class Cron(Plugin):
    """
    Run jobs at specific times and intervals using cron-like syntax.

    The schedule is persistent and dynamic, being stored in the database. This
    allows for jobs to be scheduled and rescheduled without needing to restart
    the worker(s).

    If a scheduled job is already on the queue waiting to run, or currently
    running, the job will not be queued again and instead will wait until the
    next scheduled time.

    .. note::

        While the underlying library used to parse the cron syntax supports
        timezones, this plugin does not. All times are assumed to be in UTC.
        This is due to frequent issues that occur with timezones and daylight
        saving time changes that we simply don't want to support.

    Installation
    ------------

    This plugin requires extra dependencies to parse the cron syntax. You can
    install them using:

    .. code-block:: bash

        pip install chancy[cron]

    This plugin requires a database migration to create the table that stores
    the cron-like schedules.

    Usage
    -----

    To use the cron plugin, you need to add it to your Chancy application and
    then set up the schedule for the jobs you want to run:

    .. code-block:: python
        :caption: worker.py

        import asyncio
        from chancy import Chancy, Worker, Queue
        from chancy.plugins.pruner import Cron
        from chancy.plugins.leadership import Leadership

        def hello_world():
            print("hello_world")

        async with Chancy(
            dsn="postgresql://localhost/postgres",
            plugins=[
                Queue(name="default", concurrency=10),
                Leadership(),
                Cron(),
            ],
        ) as chancy:
            await chancy.migrate()
            await Cron.schedule(
                chancy,
                "*/2 * * * *",
                Job(
                    func="worker.hello_world",
                    unique_key="hello_world_cron",
                    queue="default",
                )
            )
            await Worker(chancy).start()

    :param poll_interval: The number of seconds between cron poll intervals.
    """

    def __init__(self, *, poll_interval: int = 60):
        super().__init__()
        self.poll_interval = poll_interval

    @classmethod
    def get_scope(cls) -> PluginScope:
        return PluginScope.WORKER

    async def run(self, worker: Worker, chancy: Chancy):
        table = sql.Identifier(f"{chancy.prefix}cron")

        while await self.sleep(self.poll_interval):
            async with chancy.pool.connection() as conn:
                # We need to find every row in the {prefix}_cron table where
                # the next_run time is less than or equal to the current time,
                # lock it, update the next_run time, and then push the job onto
                # the queue.
                now = datetime.now(tz=timezone.utc)
                async with conn.cursor() as cursor:
                    async with conn.transaction():
                        await cursor.execute(
                            sql.SQL(
                                """
                                SELECT
                                    unique_key,
                                    cron,
                                    job
                                FROM {table}
                                WHERE next_run <= %(now)s
                                FOR UPDATE SKIP LOCKED
                                """
                            ).format(table=table),
                            {"now": now},
                        )

                        for row in await cursor.fetchall():
                            unique_key, cron, job = row
                            # If we're using our built-in default queue, we
                            # can push this as part of our transaction.
                            await chancy.push_many_ex(
                                cursor,
                                [Job.unpack(job)],
                            )

                            chancy.log.debug(
                                f"Pushed scheduled cron job {unique_key!r}"
                            )

                            await cursor.execute(
                                sql.SQL(
                                    """
                                    UPDATE {table}
                                    SET
                                        next_run = %(next_run)s,
                                        last_run = %(last_run)s
                                    WHERE unique_key = %(unique_key)s
                                    """
                                ).format(table=table),
                                {
                                    "next_run": croniter(cron, now).get_next(
                                        datetime
                                    ),
                                    "last_run": now,
                                    "unique_key": unique_key,
                                },
                            )

    def migrate_key(self) -> str | None:
        return "cron"

    def migrate_package(self) -> str | None:
        return "chancy.plugins.cron.migrations"

    @classmethod
    async def unschedule(cls, chancy: Chancy, *unique_keys: str):
        """
        Permanently unschedule one or more jobs from running.

        .. code-block:: python

            await Cron.unschedule(chancy, "hello_world_cron")

        :param chancy: The Chancy application.
        :param unique_keys: The unique keys of the jobs to unschedule.
        """
        async with chancy.pool.connection() as conn:
            async with conn.cursor() as cursor:
                async with conn.transaction():
                    await cursor.execute(
                        sql.SQL(
                            """
                            DELETE FROM {table}
                            WHERE unique_key = ANY(%(unique_keys)s)
                            """
                        ).format(table=sql.Identifier(f"{chancy.prefix}cron")),
                        {"unique_keys": unique_keys},
                    )

    @classmethod
    async def schedule(cls, chancy: Chancy, cron: str, *jobs: Job):
        """
        Schedule one or more jobs to run at specific times and intervals.

        All jobs that are scheduled with this feature *must* be using a
        :attr:`~chancy.job.Job.unique_key` to ensure that only one
        copy of the job is scheduled at a time. Scheduling a job with the same
        unique key as an existing job will update the existing job with the new
        schedule & job.

        For example, to run a function once every 2 minutes, we'd use:

        .. code-block:: python

            await Cron.schedule(
                chancy,
                "default",
                "*/2 * * * *",
                Job(func="worker.hello_world", unique_key="hello_world_cron")
            )

        :param chancy: The Chancy application.
        :param cron: A cron-like syntax string that describes when to run the
                     job.
        :param jobs: The jobs to run.
        """
        jobs = list(jobs)
        for job in jobs:
            if not job.unique_key:
                raise ValueError(
                    "Scheduling jobs for execution on a cron-like schedule"
                    " requires that each job has a unique_key set."
                )

        base = datetime.now(tz=timezone.utc)

        async with chancy.pool.connection() as conn:
            async with conn.cursor() as cursor:
                async with conn.transaction():
                    await cursor.executemany(
                        sql.SQL(
                            """
                            INSERT INTO {table} (
                                unique_key,
                                cron,
                                job,
                                next_run
                            )
                            VALUES (
                                %(unique_key)s,
                                %(cron)s,
                                %(job)s,
                                %(next_run)s
                            )
                            ON CONFLICT (unique_key) DO UPDATE SET
                                cron = %(cron)s,
                                job = %(job)s,
                                next_run = %(next_run)s
                            """
                        ).format(table=sql.Identifier(f"{chancy.prefix}cron")),
                        [
                            {
                                "unique_key": job.unique_key,
                                "cron": cron,
                                "job": json.dumps(job.pack()),
                                "next_run": croniter(cron, base).get_next(
                                    datetime
                                ),
                            }
                            for job in jobs
                        ],
                    )
