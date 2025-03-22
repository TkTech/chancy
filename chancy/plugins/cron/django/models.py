from django.db import models
from django.conf import settings

PREFIX = getattr(settings, "CHANCY_PREFIX", "chancy_")


class Cron(models.Model):
    unique_key = models.TextField(primary_key=True)
    job = models.JSONField(null=False)
    cron = models.TextField(null=False)
    last_run = models.DateTimeField()
    next_run = models.DateTimeField(null=False)

    class Meta:
        managed = False
        db_table = f"{PREFIX}cron"
