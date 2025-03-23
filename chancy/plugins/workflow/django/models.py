"""
Unmanaged Django models for the workflow plugin.
"""

__all__ = ("Workflow", "WorkflowStep")

from django.db import models
from django.conf import settings

from chancy.utils import chancy_uuid

PREFIX = getattr(settings, "CHANCY_PREFIX", "chancy_")


class Workflow(models.Model):
    id = models.UUIDField(primary_key=True, default=chancy_uuid, editable=False)
    name = models.TextField(null=False)
    state = models.TextField(null=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = f"{PREFIX}workflows"


class WorkflowStep(models.Model):
    id = models.AutoField(primary_key=True)
    workflow = models.ForeignKey(
        Workflow,
        on_delete=models.CASCADE,
        related_name="steps",
        db_column="workflow_id",
    )
    step_id = models.TextField(null=False)
    job_data = models.JSONField(null=False)
    dependencies = models.JSONField(null=False)
    job_id = models.UUIDField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = f"{PREFIX}workflow_steps"
        unique_together = ("workflow", "step_id")
