from django.contrib import admin
from queue_do.models import Configuration, JobQueue

admin.site.register(Configuration)
admin.site.register(JobQueue)
