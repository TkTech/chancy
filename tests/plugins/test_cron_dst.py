"""
How cron schedules behave across daylight saving time (DST) transitions.

Most tests use ``Europe/Paris`` in 2026:

- Spring, Sunday 29 March: at 02:00 (UTC+1) the clock jumps to 03:00 (UTC+2),
  so 02:00-03:00 does not exist.
- Autumn, Sunday 25 October: at 03:00 (UTC+2) the clock goes back to 02:00
  (UTC+1), so 02:00-03:00 happens twice.

Runs are written as local wall time plus UTC offset, e.g. ``02:30+0200`` is
the first 02:30 of the autumn night and ``02:30+0100`` the second one.

The rules, whatever the length of the skipped or repeated period:

- A fixed-time job (neither minute nor hour starts with ``*``) runs once:
  at the end of the skipped period when its time is skipped, in the first
  pass when it repeats.
- A wildcard job (minute or hour starts with ``*``) matches local time: it
  does not run at skipped times and runs in both passes of the repeated
  period. Elapsed intervals can change across DST.

``test_other_transitions`` checks the same rules with a 30-minute shift, a
2-hour shift and a change at midnight.

The plugin's docstring documents the same rules for users.
"""

import random
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from croniter import CroniterBadDateError, croniter

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


def poll(
    cron: str, start: datetime, end: datetime, tz: ZoneInfo = PARIS
) -> list[str]:
    """
    Replay the cron plugin's polling loop, polling once a minute from `start`
    (included) to `end` (excluded), and return the runs as wall times in `tz`
    with their UTC offset.
    """
    now = start.astimezone(UTC)
    end = end.astimezone(UTC)
    next_run = _next_run(cron, now - timedelta(seconds=1), tz)
    runs = []

    while now < end:
        if next_run <= now:
            runs.append(next_run.astimezone(tz).strftime("%H:%M%z"))
            next_run = _next_run(cron, now, tz)
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
        # A wildcard job restricted to the skipped hour has no run that day.
        ("*/15 2 * * *", []),
        ("0 */2 * * *", ["00:00+0100"]),
        ("15 */2 * * *", ["00:15+0100"]),
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


@pytest.mark.parametrize(
    "zone, cron, now_utc, expected",
    [
        (
            "Europe/Paris",
            "15 */2 * * *",
            "2026-03-29T00:00:00+00:00",
            "2026-03-29T04:15:00+02:00",
        ),
        (
            "Europe/Paris",
            "*/15 2 * * *",
            "2026-03-29T00:00:00+00:00",
            "2026-03-30T02:00:00+02:00",
        ),
        (
            "Europe/Paris",
            "15 */2 * * * 10",
            "2026-03-29T00:00:00+00:00",
            "2026-03-29T04:15:10+02:00",
        ),
        (
            "Australia/Lord_Howe",
            "*/20 2 * * *",
            "2026-10-03T14:00:00+00:00",
            "2026-10-04T02:40:00+11:00",
        ),
        # The end of a gap can itself be a valid scheduled occurrence.
        (
            "Australia/Lord_Howe",
            "*/15 2 * * *",
            "2026-10-03T14:00:00+00:00",
            "2026-10-04T02:30:00+11:00",
        ),
        (
            "Antarctica/Troll",
            "*/15 1-2 * * *",
            "2026-03-29T00:00:00+00:00",
            "2026-03-30T01:00:00+02:00",
        ),
        (
            "America/Santiago",
            "*/15 0 * * *",
            "2026-09-06T02:00:00+00:00",
            "2026-09-07T00:00:00-03:00",
        ),
    ],
)
def test_wildcard_schedules_resume_after_skipped_times(
    zone, cron, now_utc, expected
):
    now = datetime.fromisoformat(now_utc)
    next_run = _next_run(cron, now, ZoneInfo(zone))
    assert next_run.isoformat() == expected
    assert next_run.astimezone(UTC) > now


@pytest.mark.parametrize(
    "cron",
    ["@hourly", "*/15 * * * *", "* * * * * 10", "0 * * * mon#2", "R * * * *"],
)
def test_utc_schedules_preserve_croniter_behavior(cron, monkeypatch):
    now = datetime(2026, 1, 1, tzinfo=UTC)
    monkeypatch.setattr(random, "randint", random.Random(0).randint)
    expected = croniter(cron, now).get_next(datetime)
    monkeypatch.setattr(random, "randint", random.Random(0).randint)
    assert _next_run(cron, now, ZoneInfo("Etc/UTC")) == expected


def test_schedule_always_in_dst_gap_has_bounded_search():
    # Every occurrence is in Paris's missing hour on the last Sunday of March.
    with pytest.raises(CroniterBadDateError, match="within 50 years"):
        _next_run("*/15 2 * 3 L0", datetime(2026, 1, 1, tzinfo=UTC), PARIS)


@pytest.mark.parametrize(
    "cron, now, expected",
    [
        # The next leap day after 2096 is eight years away.
        ("0 0 29 2 *", "2096-03-01", "2104-02-29"),
        # A year field can place the next occurrence at the search boundary.
        ("0 0 1 1 * 0 2076", "2026-01-01", "2076-01-01"),
    ],
)
def test_sparse_schedules_remain_supported(cron, now, expected):
    result = _next_run(
        cron,
        datetime.fromisoformat(now).replace(tzinfo=UTC),
        ZoneInfo("Etc/UTC"),
    )
    assert result == datetime.fromisoformat(expected).replace(tzinfo=UTC)


# The same rules hold whatever the length or the time of the change. Each
# case polls from 3 hours before the transition to 3 hours after it.
LORD_HOWE = ZoneInfo("Australia/Lord_Howe")  # 30-minute shift
TROLL = ZoneInfo("Antarctica/Troll")  # 2-hour shift
SANTIAGO = ZoneInfo("America/Santiago")  # Change at midnight


@pytest.mark.parametrize(
    "tz, transition, cron, expected",
    [
        # Lord Howe, 4 April 2026: 02:00 (UTC+11) goes back to 01:30 (UTC+10:30).
        (
            LORD_HOWE,
            datetime(2026, 4, 4, 15, 0, tzinfo=UTC),
            "30 1 * * *",
            ["01:30+1100"],
        ),
        (
            LORD_HOWE,
            datetime(2026, 4, 4, 15, 0, tzinfo=UTC),
            "*/30 * * * *",
            [
                "23:00+1100",
                "23:30+1100",
                "00:00+1100",
                "00:30+1100",
                "01:00+1100",
                "01:30+1100",
                "01:30+1030",
                "02:00+1030",
                "02:30+1030",
                "03:00+1030",
                "03:30+1030",
                "04:00+1030",
            ],
        ),
        # Lord Howe, 3 October 2026: 02:00 (UTC+10:30) jumps to 02:30 (UTC+11).
        (
            LORD_HOWE,
            datetime(2026, 10, 3, 15, 30, tzinfo=UTC),
            "0 2 * * *",
            ["02:30+1100"],
        ),
        (
            LORD_HOWE,
            datetime(2026, 10, 3, 15, 30, tzinfo=UTC),
            "*/30 * * * *",
            [
                "23:00+1030",
                "23:30+1030",
                "00:00+1030",
                "00:30+1030",
                "01:00+1030",
                "01:30+1030",
                "02:30+1100",
                "03:00+1100",
                "03:30+1100",
                "04:00+1100",
                "04:30+1100",
                "05:00+1100",
            ],
        ),
        # Troll, 29 March 2026: 01:00 (UTC+0) jumps to 03:00 (UTC+2).
        (
            TROLL,
            datetime(2026, 3, 29, 1, 0, tzinfo=UTC),
            "0 1 * * *",
            ["03:00+0200"],
        ),
        (
            TROLL,
            datetime(2026, 3, 29, 1, 0, tzinfo=UTC),
            "*/30 * * * *",
            [
                "22:00+0000",
                "22:30+0000",
                "23:00+0000",
                "23:30+0000",
                "00:00+0000",
                "00:30+0000",
                "03:00+0200",
                "03:30+0200",
                "04:00+0200",
                "04:30+0200",
                "05:00+0200",
                "05:30+0200",
            ],
        ),
        # Troll, 25 October 2026: 03:00 (UTC+2) goes back to 01:00 (UTC+0).
        (
            TROLL,
            datetime(2026, 10, 25, 1, 0, tzinfo=UTC),
            "0 1 * * *",
            ["01:00+0200"],
        ),
        (
            TROLL,
            datetime(2026, 10, 25, 1, 0, tzinfo=UTC),
            "*/30 * * * *",
            [
                "00:00+0200",
                "00:30+0200",
                "01:00+0200",
                "01:30+0200",
                "02:00+0200",
                "02:30+0200",
                "01:00+0000",
                "01:30+0000",
                "02:00+0000",
                "02:30+0000",
                "03:00+0000",
                "03:30+0000",
            ],
        ),
        # Santiago, 5 April 2026: midnight (UTC-3) goes back to 23:00 (UTC-4).
        (
            SANTIAGO,
            datetime(2026, 4, 5, 3, 0, tzinfo=UTC),
            "0 23 * * *",
            ["23:00-0300"],
        ),
        (
            SANTIAGO,
            datetime(2026, 4, 5, 3, 0, tzinfo=UTC),
            "*/30 * * * *",
            [
                "21:00-0300",
                "21:30-0300",
                "22:00-0300",
                "22:30-0300",
                "23:00-0300",
                "23:30-0300",
                "23:00-0400",
                "23:30-0400",
                "00:00-0400",
                "00:30-0400",
                "01:00-0400",
                "01:30-0400",
            ],
        ),
        # Santiago, 6 September 2026: midnight (UTC-4) jumps to 01:00 (UTC-3),
        # so a daily job at midnight runs at 01:00 that day.
        (
            SANTIAGO,
            datetime(2026, 9, 6, 4, 0, tzinfo=UTC),
            "@daily",
            ["01:00-0300"],
        ),
        (
            SANTIAGO,
            datetime(2026, 9, 6, 4, 0, tzinfo=UTC),
            "*/30 * * * *",
            [
                "21:00-0400",
                "21:30-0400",
                "22:00-0400",
                "22:30-0400",
                "23:00-0400",
                "23:30-0400",
                "01:00-0300",
                "01:30-0300",
                "02:00-0300",
                "02:30-0300",
                "03:00-0300",
                "03:30-0300",
            ],
        ),
    ],
)
def test_other_transitions(tz, transition, cron, expected):
    start = transition - timedelta(hours=3)
    end = transition + timedelta(hours=3)
    assert poll(cron, start, end, tz) == expected
