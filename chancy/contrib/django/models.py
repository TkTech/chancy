"""
Unmanaged Django models that match the Chancy tables.

Makes the assumption that the Django default database is the same as the Chancy
database.
"""

__all__ = ("Job", "Worker", "Queue")

from django.contrib.postgres.fields import ArrayField
from django.db import models
from django.conf import settings

from chancy.utils import chancy_uuid

PREFIX = getattr(settings, "CHANCY_PREFIX", "chancy_")


class Job(models.Model):
    id = models.UUIDField(primary_key=True, default=chancy_uuid, editable=False)
    queue = models.TextField()
    func = models.TextField()
    kwargs = models.JSONField(db_default="{}", blank=True)
    limits = models.JSONField(db_default="[]", blank=True)
    meta = models.JSONField(db_default="{}", blank=True)
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
    errors = models.JSONField(db_default="[]", blank=True)

    class Meta:
        managed = False
        db_table = f"{PREFIX}jobs"


class Worker(models.Model):
    worker_id = models.TextField(primary_key=True)
    tags = ArrayField(models.TextField(), default=list, blank=True)
    queues = ArrayField(models.TextField(), default=list, blank=True)

    last_seen = models.DateTimeField(auto_now=True)
    expires_at = models.DateTimeField()

    class Meta:
        managed = False
        db_table = f"{PREFIX}workers"


class Queue(models.Model):
    name = models.TextField(primary_key=True)
    state = models.TextField(default="active")
    concurrency = models.IntegerField(null=True)
    tags = ArrayField(models.TextField(), default=list)
    executor = models.TextField(
        default="chancy.executors.process.ProcessExecutor"
    )
    executor_options = models.JSONField(db_default="{}", blank=True)
    polling_interval = models.IntegerField(default=5)
    rate_limit = models.IntegerField(null=True, blank=True)
    rate_limit_window = models.IntegerField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = False
        db_table = f"{PREFIX}queues"
