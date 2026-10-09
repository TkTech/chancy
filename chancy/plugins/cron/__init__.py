import json
from copy import copy
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import CroniterBadDateError, croniter
from psycopg import sql
from psycopg.rows import dict_row

from chancy.app import Chancy
from chancy.job import IsAJob, Job, SerializedJob
from chancy.plugin import Plugin
from chancy.worker import Worker

DEFAULT_TIMEZONE = ZoneInfo("Etc/UTC")


def _is_repeated_wall_time(dt: datetime) -> bool:
    """
    Whether `dt` is the second occurrence of a wall time that happens twice
    because daylight saving time ended, e.g. 02:30+01:00 on the last Sunday of
    October in Europe/Paris.
    """
    first_offset = dt.replace(fold=0).utcoffset()
    return (
        first_offset > dt.replace(fold=1).utcoffset()
        and dt.utcoffset() != first_offset
    )


def _next_run(cron: str, now: datetime, tz: ZoneInfo) -> datetime:
    """
    Get the first time after `now` at which `cron`, evaluated in `tz`, fires.

    See the "Daylight saving time" section of :class:`Cron` for the behaviour
    around DST transitions.
    """
    start = now.astimezone(tz)
    max_years = 50
    it = croniter(cron, start, max_years_between_matches=max_years)

    minute, hour = it.expressions[:2]
    fixed_time = not (minute.startswith("*") or hour.startswith("*"))

    while True:
        next_run = it.get_next(datetime)
        # Croniter bounds each search separately. Keep rejected DST candidates
        # from extending the overall search indefinitely.
        if next_run.year > start.year + max_years:
            raise CroniterBadDateError(
                f"No usable cron occurrence within {max_years} years."
            )
        if fixed_time:
            # A fixed-time job runs only once when its time repeats.
            if _is_repeated_wall_time(next_run):
                continue
        else:
            # Croniter may move a missing time to the end of the DST gap.
            # Wildcard jobs must still match the actual local clock time.
            # Check without timezone adjustments, reusing the parsed expression
            # so random fields keep their original values.
            wall_time = next_run.replace(tzinfo=None)
            previous = copy(it).get_prev(
                datetime, start_time=wall_time + timedelta(microseconds=1)
            )
            if previous != wall_time:
                continue
        return next_run


class Cron(Plugin):
    """
    Run jobs at specific times and intervals using cron-like syntax.

    The schedule is persistent and dynamic, being stored in the database. This
    allows for jobs to be scheduled and rescheduled without needing to restart
    the worker(s).

    If a scheduled job is already on the queue waiting to run, or currently
    running, the job will not be queued again and instead will wait until the
    next scheduled time.

    Timezones
    ---------

    By default, cron expressions are evaluated in UTC. Each schedule can
    instead be evaluated in its own IANA timezone by passing ``timezone`` to
    :meth:`schedule`:

    .. code-block:: python

        from zoneinfo import ZoneInfo

        # Every day at 09:00 Paris time, whether it is winter or summer.
        await Cron.schedule(
            chancy,
            "0 9 * * *",
            hello_world.job.with_unique_key("hello_world_paris"),
            timezone=ZoneInfo("Europe/Paris"),
        )

    Daylight saving time
    ~~~~~~~~~~~~~~~~~~~~

    UTC, the default, and timezones without daylight saving time (DST), such
    as ``Asia/Tokyo``, are never affected by this section.

    In a timezone that observes DST, the local clock skips a period once a
    year and repeats one once a year. How long that period is, often one
    hour, and when it happens depend on the timezone. In ``Europe/Paris``,
    02:00-03:00 does not exist on the last Sunday of March and happens twice
    on the last Sunday of October (first at UTC+2, then at UTC+1). In
    ``Australia/Lord_Howe`` the period is 30 minutes, and in
    ``America/Santiago`` the change happens at midnight. Schedules outside
    the skipped and repeated periods are unaffected and keep their local time
    all year round.

    Inside those periods, the behaviour depends on whether the job runs at a
    *fixed time* or uses a *wildcard*:

    - A **fixed-time** job has neither its minute nor its hour field starting
      with ``*``, e.g. ``30 2 * * *``, ``0 2 * * 1-5`` or ``@daily``.
    - A **wildcard** job has its minute or its hour field starting with
      ``*``, e.g. ``* * * * *``, ``*/15 2 * * *``, ``0 * * * *``,
      ``0 */2 * * *`` or ``@hourly``.

    These are the classes, and the rules below, that Vixie cron and its
    descendants such as Debian's cron use for daylight saving time changes
    (see `cron(8) <https://manpages.debian.org/bookworm/cron/cron.8.en.html>`_).
    Like cron, the class depends on how the fields are written, not on the
    times they match. ``0 */2 * * *`` is a wildcard job, but the equivalent
    ``0 0/2 * * *`` is a fixed-time job, and so is ``0 0-23 * * *`` while
    ``0 * * * *`` is a wildcard job.

    .. list-table::
        :header-rows: 1

        * - Transition (example: ``Europe/Paris``)
          - Fixed-time job (``30 2 * * *``)
          - Wildcard job (``*/30 * * * *``)
        * - Skipped period (02:00-03:00 in spring)
          - Runs **once, at the end of the skipped period** (03:00), then at
            02:30 again from the next day.
          - Runs at the times that exist: 01:30, then 03:00. The skipped
            02:00 and 02:30 do not run. A job restricted to the skipped
            period, like ``*/15 2 * * *``, does not run that day.
        * - Repeated period (02:00-03:00 in autumn)
          - Runs **once**, during the first pass (02:30 UTC+2). The second
            02:30 (UTC+1) does not run.
          - Matches local times in **both passes**: 02:00 and 02:30
            UTC+2, then 02:00 and 02:30 UTC+1.

    A fixed-time job does not repeat an occurrence because of DST. Wildcard
    jobs match the local clock, so elapsed intervals can change across DST.
    For example, ``15 */2 * * *`` runs at 00:15 and then 04:15 when 02:15
    is skipped; it does not catch up at 03:00.
    ``tests/plugins/test_cron_dst.py`` covers these rules for
    ``Europe/Paris``, ``Australia/Lord_Howe`` (30-minute shift),
    ``Antarctica/Troll`` (2-hour shift) and ``America/Santiago`` (change at
    midnight).

    Limitations
    ~~~~~~~~~~~

    - Some wildcard schedules can miss occurrences during the repeated DST
      period due to a croniter limitation. For example, ``*/7 * * * *`` can
      skip the second pass in ``Australia/Lord_Howe``.
    - A schedule must have a usable occurrence within the next 50 calendar
      years. Schedules without one are rejected when saved and skipped with
      an error in the logs when already stored.
    - ``next_run`` is computed when a schedule is saved through
      :meth:`schedule` and each time it runs. Editing the expression or the
      timezone directly in the database, including through the Django admin,
      only takes effect after the next run.
    - The timezone column must hold a valid IANA name, and the expression
      must be valid. The Django admin only offers valid timezones, but does
      not validate expressions. A due schedule with an invalid timezone or
      expression is skipped, logging an error on every poll until it is
      fixed.
    - Versions of Chancy without timezone support ignore the column and
      evaluate every schedule in UTC. While workers of both versions run side
      by side, or after a downgrade, schedules in another timezone can fire
      at the UTC reading of their expression.
    - The dashboard's timeline computes upcoming runs in the browser. It uses
      each schedule's timezone, but around a DST transition it can differ
      from the actual runs: it may omit executions or show extra or shifted
      ones. The Next Run value comes from the server's stored schedule.

    Installation
    ------------

    This plugin requires an extra dependency to parse the cron syntax. You can
    install it using:

    .. code-block:: bash

        pip install chancy[cron]

    This plugin requires a database migration to create the table that stores
    the cron-like schedules.

    Usage
    -----

    To use the cron plugin, you need to add it to your Chancy application and
    then set up the schedule for the jobs you want to run:

    .. code-block:: python

        import asyncio
        from chancy import Chancy, Worker, Queue, job
        from chancy.plugins.cron import Cron

        @job(queue="default")
        def hello_world():
            print("hello_world")

        async with Chancy(
            "postgresql://localhost/postgres",
            plugins=[Cron()]
        ) as chancy:
            await Cron.schedule(
                chancy,
                "*/2 * * * *",
                hello_world.job.with_unique_key("hello_world_cron")
            )

    Django Integration
    ------------------

    This plugin can be made available to the Django ORM and Admin interface.

    To enable this, you need to add the following to your Django settings:

    .. code-block:: python

        INSTALLED_APPS = [
            ...,
            "chancy.plugins.cron.django",
        ]

    You can then query the scheduled jobs using the Django ORM:

    .. code-block:: python

        from chancy.plugins.cron.django.models import Cron

        # Get all scheduled jobs
        all_schedules = Cron.objects.all()

    :param poll_interval: The number of seconds between cron poll intervals.
    """

    def __init__(self, *, poll_interval: int = 60):
        super().__init__()
        self.poll_interval = poll_interval

    async def run(self, worker: Worker, chancy: Chancy):
        table = sql.Identifier(f"{chancy.prefix}cron")

        while await self.sleep(self.poll_interval):
            async with chancy.pool.connection() as conn:
                # We need to find every row in the {prefix}_cron table where
                # the next_run time is less than or equal to the current time,
                # lock it, update the next_run time, and then push the job onto
                # the queue.
                now = datetime.now(tz=UTC)
                async with (
                    conn.cursor(row_factory=dict_row) as cursor,
                    conn.transaction(),
                ):
                    await cursor.execute(
                        sql.SQL(
                            """
                                SELECT
                                    unique_key,
                                    cron,
                                    job,
                                    timezone
                                FROM {table}
                                WHERE next_run <= %(now)s
                                FOR UPDATE SKIP LOCKED
                                """
                        ).format(table=table),
                        {"now": now},
                    )

                    for row in await cursor.fetchall():
                        # Compute the next run before pushing anything, so a
                        # schedule with an invalid expression or timezone is
                        # skipped instead of rolling back every due schedule.
                        try:
                            next_run = _next_run(
                                row["cron"],
                                now,
                                ZoneInfo(row["timezone"]),
                            )
                        except (ValueError, ZoneInfoNotFoundError):
                            chancy.log.exception(
                                f"Skipping cron job {row['unique_key']!r},"
                                f" unable to compute its next run from"
                                f" {row['cron']!r} in timezone"
                                f" {row['timezone']!r}."
                            )
                            continue

                        # If we're using our built-in default queue, we
                        # can push this as part of our transaction.
                        await chancy.push_many_ex(
                            cursor,
                            [SerializedJob.unpack(row["job"])],
                        )

                        chancy.log.debug(
                            f"Pushed scheduled cron job {row['unique_key']!r}"
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
                                "next_run": next_run,
                                "last_run": now,
                                "unique_key": row["unique_key"],
                            },
                        )

    def migrate_key(self) -> str | None:
        return "cron"

    def migrate_package(self) -> str | None:
        return "chancy.plugins.cron.migrations"

    def api_plugin(self) -> str | None:
        return "chancy.plugins.cron.api.CronApiPlugin"

    def get_tables(self) -> list[str]:
        """Get the names of all tables this plugin is responsible for."""
        return ["cron"]

    @staticmethod
    def get_identifier() -> str:
        return "chancy.cron"

    @classmethod
    async def get_schedules(
        cls, chancy: Chancy, *, unique_keys: list[str] | None = None
    ) -> dict[str, dict]:
        """
        Get scheduled cron jobs by their unique keys.

        If no unique keys are provided, all scheduled jobs will be returned.

        .. code-block:: python

            # Get all scheduled jobs
            all_schedules = await Cron.get_schedules(chancy)

            # Get a specific job
            job_schedule = await Cron.get_schedules(chancy, ["hello_world_cron"])

        :param chancy: The Chancy application.
        :param unique_keys: Optional list of unique keys to filter by.
        :return: The schedules, keyed by unique key. Each one is a dictionary
                 with ``unique_key``, ``job``, ``cron``, ``timezone`` (its
                 IANA name), ``last_run`` and ``next_run``.
        """
        table = sql.Identifier(f"{chancy.prefix}cron")

        async with (
            chancy.pool.connection() as conn,
            conn.cursor(row_factory=dict_row) as cursor,
        ):
            await cursor.execute(
                sql.SQL(
                    """
                        SELECT
                            unique_key,
                            job,
                            cron,
                            timezone,
                            last_run,
                            next_run
                        FROM {table}
                        WHERE 
                            (%(unique_keys)s::text[] IS NULL
                                OR unique_key = ANY(%(unique_keys)s))
                        """
                ).format(table=table),
                {"unique_keys": unique_keys},
            )

            return {
                result["unique_key"]: {
                    "unique_key": result["unique_key"],
                    "job": SerializedJob.unpack(result["job"]),
                    "cron": result["cron"],
                    "timezone": result["timezone"],
                    "last_run": result["last_run"],
                    "next_run": result["next_run"],
                }
                async for result in cursor
            }

    @classmethod
    async def unschedule(cls, chancy: Chancy, *unique_keys: str):
        """
        Permanently unschedule one or more jobs from running.

        .. code-block:: python

            await Cron.unschedule(chancy, "hello_world_cron")

        :param chancy: The Chancy application.
        :param unique_keys: The unique keys of the jobs to unschedule.
        """
        async with (
            chancy.pool.connection() as conn,
            conn.cursor(row_factory=dict_row) as cursor,
            conn.transaction(),
        ):
            await cursor.execute(
                sql.SQL(
                    """
                            DELETE FROM {table}
                            WHERE unique_key = ANY(%(unique_keys)s)
                            """
                ).format(table=sql.Identifier(f"{chancy.prefix}cron")),
                {"unique_keys": list(unique_keys)},
            )

    @classmethod
    async def schedule(
        cls,
        chancy: Chancy,
        cron: str,
        *jobs: Job | IsAJob,
        timezone: ZoneInfo = DEFAULT_TIMEZONE,
    ):
        """
        Schedule one or more jobs to run periodically based on a cron schedule.

        All jobs that are scheduled with this feature *must* be using a
        :attr:`~chancy.job.Job.unique_key` to ensure that only one
        copy of the job is scheduled at a time. Scheduling a job with the same
        unique key as an existing job will update the existing job with the new
        schedule, job & timezone.

        :param chancy: The Chancy application.
        :param cron: A cron-like syntax string that describes when to run the
                     job.
        :param jobs: The jobs to run, validated now and when they run.
        :param timezone: The timezone in which to evaluate the cron
                         expression. Defaults to UTC.
        """
        jobs = [chancy.serialize(job) for job in jobs]
        for job in jobs:
            if not job.unique_key:
                raise ValueError(
                    "Scheduling jobs for execution on a cron-like schedule"
                    " requires that each job has a unique_key set."
                )

        next_run = _next_run(cron, datetime.now(tz=UTC), timezone)

        async with (
            chancy.pool.connection() as conn,
            conn.cursor(row_factory=dict_row) as cursor,
            conn.transaction(),
        ):
            await cursor.executemany(
                sql.SQL(
                    """
                            INSERT INTO {table} (
                                unique_key,
                                cron,
                                job,
                                timezone,
                                next_run
                            )
                            VALUES (
                                %(unique_key)s,
                                %(cron)s,
                                %(job)s,
                                %(timezone)s,
                                %(next_run)s
                            )
                            ON CONFLICT (unique_key) DO UPDATE SET
                                cron = %(cron)s,
                                job = %(job)s,
                                timezone = %(timezone)s,
                                next_run = %(next_run)s
                            """
                ).format(table=sql.Identifier(f"{chancy.prefix}cron")),
                [
                    {
                        "unique_key": job.unique_key,
                        "cron": cron,
                        "job": json.dumps(job.pack()),
                        "timezone": timezone.key,
                        "next_run": next_run,
                    }
                    for job in jobs
                ],
            )
