from django.contrib import admin
from queue_do.models import Configuration, JobQueue

admin.site.register(Configuration)

class JobQueueAdmin(admin.ModelAdmin):
    list_display = ['priority', 'input_file', 'added', 'duration', 'status']
    list_display_links = ['input_file', ]
    search_fields = ['input_file', ]
    list_filter = ['status', 'configuration', 'priority']
    readonly_fields = ['added', 'modified', 'duration']
admin.site.register(JobQueue, JobQueueAdmin)
