from zoneinfo import available_timezones

from django.conf import settings
from django.db import models

PREFIX = getattr(settings, "CHANCY_PREFIX", "chancy_")

# "localtime" is the host's own timezone on some systems, not an IANA name
# that every worker can load.
TIMEZONE_CHOICES = [
    (tz, tz) for tz in sorted(available_timezones() - {"localtime"})
]


class Cron(models.Model):
    unique_key = models.TextField(primary_key=True)
    job = models.JSONField(null=False)
    cron = models.TextField(null=False)
    timezone = models.TextField(
        null=False, default="Etc/UTC", choices=TIMEZONE_CHOICES
    )
    last_run = models.DateTimeField()
    next_run = models.DateTimeField(null=False)

    class Meta:
        managed = False
        db_table = f"{PREFIX}cron"
