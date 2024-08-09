from datetime import datetime, timezone


def time_until(future_date):
    now = datetime.now(timezone.utc)
    if isinstance(future_date, str):
        future_date = datetime.fromisoformat(future_date.replace("Z", "+00:00"))

    if future_date < now:
        return "Date has passed"

    delta = future_date - now
    days = delta.days
    hours, rem = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(rem, 60)

    parts = []
    if days > 0:
        parts.append(f"{days} day{'s' if days != 1 else ''}")
    if hours > 0:
        parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
    if minutes > 0:
        parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
    if seconds > 0:
        parts.append(f"{seconds} second{'s' if seconds != 1 else ''}")

    if len(parts) == 0:
        return "Now"
    elif len(parts) == 1:
        return parts[0]
    elif len(parts) == 2:
        return f"{parts[0]} and {parts[1]}"
    else:
        return ", ".join(parts[:-1]) + f", and {parts[-1]}"


def relative_time(dt: datetime | None) -> str:
    """
    Show the time in hours, minutes, and seconds between `dt` and now.

    Works with both past and future datetimes. Returns a string that looks like
    HHhMMmSSs, where HH is hours, MM is minutes, and SS is seconds. If the time
    is within an hour, only minutes and seconds are shown. If the time is within
    a minute, only seconds are shown.
    """
    if dt is None:
        return "-"

    now = datetime.now(timezone.utc)
    delta = abs(now - dt)
    hours, rem = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(rem, 60)

    p = []
    if delta.days > 0:
        p.append(f"{delta.days}d")
    if hours > 0:
        p.append(f"{hours}h")
    if minutes > 0:
        p.append(f"{minutes}m")
    if seconds > 0:
        p.append(f"{seconds}s")

    return f"{''.join(p)} ago" if dt < now else f"in {''.join(p)}"
