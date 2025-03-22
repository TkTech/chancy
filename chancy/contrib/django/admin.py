from django.contrib import admin

from chancy.contrib.django.models import Job, Worker


class ReadOnlyAdmin(admin.ModelAdmin):
    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    # Oddly, this prevents the user from viewing the object.
    # def has_change_permission(self, request, obj=None):
    #     return False


@admin.register(Job)
class JobAdmin(admin.ModelAdmin):
    list_display = ("id", "queue", "func", "state", "attempts", "max_attempts")
    search_fields = ("id", "queue", "func")


@admin.register(Worker)
class WorkerAdmin(ReadOnlyAdmin):
    list_display = ("worker_id", "tags", "queues")
    search_fields = ("worker_id",)
    list_filter = ("tags", "queues")
