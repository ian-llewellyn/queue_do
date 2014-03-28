from django.contrib import admin
from queue_do.models import Configuration, JobQueue

admin.site.register(Configuration)

class JobQueueAdmin(admin.ModelAdmin):
    list_display = ['priority', 'input_file', 'added', 'duration', 'status', 'failure_count']
    list_display_links = ['input_file', ]
    search_fields = ['input_file', ]
    list_filter = ['status', 'configuration', 'priority', 'failure_count']
    readonly_fields = ['error', 'added', 'modified', 'duration']

    # Set a selection of actions to state queued
    actions = ['set_queueing']

    def set_queueing(self, request, queryset):
        queryset.update(status="queued")
    set_queueing.short_description = "Queued selected jobs"

admin.site.register(JobQueue, JobQueueAdmin)
