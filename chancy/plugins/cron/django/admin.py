from django.contrib import admin

from chancy.plugins.cron.django.models import Cron


@admin.register(Cron)
class CronAdmin(admin.ModelAdmin):
    list_display = ("unique_key", "cron", "last_run", "next_run")
    search_fields = ("unique_key", "cron")
