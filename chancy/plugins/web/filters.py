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


def relative_time(dt):
    """
    Show the time (in minutes and seconds) since or until the given datetime.
    """
    if dt is None:
        return "-"

    now = datetime.now(timezone.utc)
    delta = dt - now

    hours, rem = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(rem, 60)

    if hours > 0:
        return f"{hours:02}h{minutes:02}m{seconds:02}s"
    elif minutes > 0:
        return f"{minutes:02}m{seconds:02}s"
    else:
        return f"{seconds:02}s"
