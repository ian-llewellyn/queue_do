from django.db import models

# Create your models here.

class Configuration(models.Model):
    watch_dir = models.CharField(max_length=100, blank=False, null=False,
        help_text='The directory for inotifywait to watch.')
    filter = models.CharField(max_length=100, blank=False, null=False,
        help_text='Regular expression to stop certain entries making it to the queue.')
    run_script = models.CharField(max_length=100, blank=False, null=False,
        help_text='The script that should be run when the job is being processed.')
    default_priority = models.PositiveIntegerField(blank=False, null=False,
        help_text='The default priority of jobs in this queue: 0 = highest priority.')

    def __unicode__(self):
        return self.watch_dir

class JobQueue(models.Model):
    input_file = models.CharField(max_length=100, blank=False, null=False,
        help_text='The file that was captured by inotfywait and made it through the filter.')
    priority = models.PositiveIntegerField(blank=False, null=False,
        help_text='The priority of this job: 0 = highest priority.')
    added = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)
    status = models.CharField(max_length=30, choices=[
        ('queued', 'Waiting in Queue'),
        ('processing', 'Processing'),
        ('success', 'Completed'),
        ('fail', 'Failed')])

    def __unicode__(self):
        return self.input_file + ': ' + self.status
