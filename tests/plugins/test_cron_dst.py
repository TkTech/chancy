"""
How cron schedules behave across daylight saving time (DST) transitions.

Every test uses ``Europe/Paris`` in 2026:

- Spring, Sunday 29 March: at 02:00 (UTC+1) the clock jumps to 03:00 (UTC+2),
  so 02:00-03:00 does not exist.
- Autumn, Sunday 25 October: at 03:00 (UTC+2) the clock goes back to 02:00
  (UTC+1), so 02:00-03:00 happens twice.

Runs are written as local wall time plus UTC offset, e.g. ``02:30+0200`` is
the first 02:30 of the autumn night and ``02:30+0100`` the second one.

The rules:

- A fixed-time job (neither minute nor hour starts with ``*``) runs once:
  at 03:00 when its time is skipped, in the first pass when it repeats.
- A wildcard job (minute or hour starts with ``*``) follows real time: it
  does not run at skipped times and runs in both passes of the repeated hour.
  Exception: one restricted to the skipped hour runs once at 03:00.

The plugin's docstring documents the same rules for users.
"""

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from chancy.plugins.cron import _next_run

PARIS = ZoneInfo("Europe/Paris")

# From 00:00 (included) to 04:00 (excluded) Paris time on each transition
# night: 3 real hours in spring, 5 in autumn.
SPRING_NIGHT = (
    datetime(2026, 3, 29, 0, 0, tzinfo=PARIS),
    datetime(2026, 3, 29, 4, 0, tzinfo=PARIS),
)
AUTUMN_NIGHT = (
    datetime(2026, 10, 25, 0, 0, tzinfo=PARIS),
    datetime(2026, 10, 25, 4, 0, tzinfo=PARIS),
)


def poll(cron: str, start: datetime, end: datetime) -> list[str]:
    """
    Replay the cron plugin's polling loop, polling once a minute from `start`
    (included) to `end` (excluded), and return the runs as Paris wall times
    with their UTC offset.
    """
    now = start.astimezone(UTC)
    end = end.astimezone(UTC)
    next_run = _next_run(cron, now - timedelta(seconds=1), PARIS)
    runs = []

    while now < end:
        if next_run <= now:
            runs.append(next_run.astimezone(PARIS).strftime("%H:%M%z"))
            next_run = _next_run(cron, now, PARIS)
        now += timedelta(minutes=1)

    return runs


@pytest.mark.parametrize(
    "cron, expected",
    [
        # Fixed-time jobs: a time in the skipped hour runs once, at 03:00.
        ("30 2 * * *", ["03:00+0200"]),
        ("0 2 * * *", ["03:00+0200"]),
        ("30 1-3 * * *", ["01:30+0100", "03:00+0200", "03:30+0200"]),
        # Fixed-time jobs outside the skipped hour are unaffected.
        ("0 1 * * *", ["01:00+0100"]),
        ("0 3 * * *", ["03:00+0200"]),
        # Wildcard jobs: the times that do not exist are not run.
        ("0 * * * *", ["00:00+0100", "01:00+0100", "03:00+0200"]),
        ("@hourly", ["00:00+0100", "01:00+0100", "03:00+0200"]),
        (
            "*/30 * * * *",
            [
                "00:00+0100",
                "00:30+0100",
                "01:00+0100",
                "01:30+0100",
                "03:00+0200",
                "03:30+0200",
            ],
        ),
        # Exception: a wildcard job restricted to the skipped hour runs once,
        # at 03:00, rather than not at all.
        ("*/15 2 * * *", ["03:00+0200"]),
    ],
)
def test_spring_skipped_hour(cron, expected):
    assert poll(cron, *SPRING_NIGHT) == expected


@pytest.mark.parametrize(
    "cron, expected",
    [
        # Fixed-time jobs: a time in the repeated hour runs once, in the
        # first pass (UTC+2), never again in the second one (UTC+1).
        ("30 2 * * *", ["02:30+0200"]),
        ("0 2 * * *", ["02:00+0200"]),
        ("30 1-3 * * *", ["01:30+0200", "02:30+0200", "03:30+0100"]),
        # Fixed-time jobs outside the repeated hour are unaffected.
        ("0 1 * * *", ["01:00+0200"]),
        # Wildcard jobs follow real time and run in both passes.
        (
            "0 * * * *",
            [
                "00:00+0200",
                "01:00+0200",
                "02:00+0200",
                "02:00+0100",
                "03:00+0100",
            ],
        ),
        (
            "@hourly",
            [
                "00:00+0200",
                "01:00+0200",
                "02:00+0200",
                "02:00+0100",
                "03:00+0100",
            ],
        ),
        (
            "*/30 * * * *",
            [
                "00:00+0200",
                "00:30+0200",
                "01:00+0200",
                "01:30+0200",
                "02:00+0200",
                "02:30+0200",
                "02:00+0100",
                "02:30+0100",
                "03:00+0100",
                "03:30+0100",
            ],
        ),
        (
            "*/15 2 * * *",
            [
                "02:00+0200",
                "02:15+0200",
                "02:30+0200",
                "02:45+0200",
                "02:00+0100",
                "02:15+0100",
                "02:30+0100",
                "02:45+0100",
            ],
        ),
    ],
)
def test_autumn_repeated_hour(cron, expected):
    assert poll(cron, *AUTUMN_NIGHT) == expected


def test_fixed_time_job_runs_once_a_day_across_both_transitions():
    """
    A daily job inside the transition hour runs exactly once per day, from the
    day before each transition to the day after.
    """
    for night in (SPRING_NIGHT, AUTUMN_NIGHT):
        start = night[0] - timedelta(days=1)
        end = night[0] + timedelta(days=2)
        assert len(poll("30 2 * * *", start, end)) == 3


def test_every_minute_job_follows_real_time():
    """
    ``* * * * *`` runs once per real minute: from 00:00 to 04:00 on the wall
    clock, that is 3 hours of runs in spring and 5 in autumn.
    """
    assert len(poll("* * * * *", *SPRING_NIGHT)) == 3 * 60
    assert len(poll("* * * * *", *AUTUMN_NIGHT)) == 5 * 60


@pytest.mark.parametrize(
    "now_utc, cron, expected",
    [
        # A worker (re)starting in the middle of the repeated hour, e.g.
        # during a deploy, gets the right next run.
        ("00:40", "0 * * * *", "02:00+0100"),
        ("01:10", "*/30 * * * *", "02:30+0100"),
        ("01:10", "30 2 * * *", "02:30+0100 on 26"),
        ("01:40", "*/15 2 * * *", "02:45+0100"),
    ],
)
def test_next_run_from_inside_the_repeated_hour(now_utc, cron, expected):
    hour, minute = map(int, now_utc.split(":"))
    now = datetime(2026, 10, 25, hour, minute, tzinfo=UTC)

    next_run = _next_run(cron, now, PARIS)

    assert next_run > now
    local = next_run.astimezone(PARIS)
    formatted = local.strftime("%H:%M%z")
    if local.day != 25:
        formatted += f" on {local.day}"
    assert formatted == expected
