from django.contrib import admin

from chancy.plugins.workflow.django.models import Workflow, WorkflowStep


class WorkflowStepInline(admin.TabularInline):
    model = WorkflowStep
    readonly_fields = (
        "id",
        "step_id",
        "dependencies",
        "job_id",
        "created_at",
        "updated_at",
    )
    extra = 0
    can_deletes = False


@admin.register(Workflow)
class WorkflowAdmin(admin.ModelAdmin):
    list_display = ("id", "name", "state", "created_at", "updated_at")
    search_fields = ("id", "name", "state")
    readonly_fields = ("id", "created_at", "updated_at")
    inlines = [WorkflowStepInline]


@admin.register(WorkflowStep)
class WorkflowStepAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "workflow",
        "step_id",
        "job_id",
        "created_at",
        "updated_at",
    )
    search_fields = (
        "id",
        "workflow__id",
        "workflow__name",
        "step_id",
        "job_id",
    )
    raw_id_fields = ("workflow",)
    readonly_fields = ("id", "created_at", "updated_at")
