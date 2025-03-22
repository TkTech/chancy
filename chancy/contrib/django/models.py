"""
Unmanaged Django models that match the Chancy tables.

Makes the assumption that the Django default database is the same as the Chancy
database.
"""

__all__ = ("Job", "Worker")

from django.contrib.postgres.fields import ArrayField
from django.db import models
from django.conf import settings

from chancy.utils import chancy_uuid

PREFIX = getattr(settings, "CHANCY_PREFIX", "chancy_")


class Job(models.Model):
    id = models.UUIDField(primary_key=True, default=chancy_uuid, editable=False)
    queue = models.TextField()
    func = models.TextField()
    kwargs = models.JSONField(db_default="{}")
    limits = models.JSONField(db_default="[]")
    meta = models.JSONField(db_default="{}")
    state = models.CharField(max_length=25, default="pending")
    priority = models.IntegerField(default=0)
    attempts = models.IntegerField(default=0)
    max_attempts = models.IntegerField(default=1)
    taken_by = models.TextField(null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    started_at = models.DateTimeField(null=True)
    completed_at = models.DateTimeField(null=True)
    scheduled_at = models.DateTimeField(auto_now_add=True)
    unique_key = models.TextField(null=True)
    errors = models.JSONField(db_default="[]")

    class Meta:
        managed = False
        db_table = f"{PREFIX}jobs"


class Worker(models.Model):
    worker_id = models.TextField(primary_key=True)
    tags = ArrayField(models.TextField(), default=list)
    queues = ArrayField(models.TextField(), default=list)

    last_seen = models.DateTimeField(auto_now=True)
    expires_at = models.DateTimeField()

    class Meta:
        managed = False
        db_table = f"{PREFIX}workers"
