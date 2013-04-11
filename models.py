from django.db import models
import subprocess # call
import select # select
import affinity # set_process_affinity_mask

# Create your models here.

class Configuration(models.Model):
    watch_dir = models.CharField(max_length=100, blank=False, null=False,
        help_text='The directory for inotifywait to watch.')
    filter = models.CharField(max_length=100, blank=False, null=False,
        help_text='Regular expression to stop certain entries making it to the queue.')
    run_script = models.CharField(max_length=100, blank=False, null=False,
        help_text='The script that should be run when the job is processed. The detected file will be passed in as the last argument.')
    default_priority = models.PositiveIntegerField(blank=False, null=False,
        help_text='The default priority of jobs in this queue: 0 = highest priority.')

    def __unicode__(self):
        return self.watch_dir

class JobQueue(models.Model):
    JOB_QUEUE_STATUS_CHOICES = [
        ('queued', 'Waiting in Queue'),
        ('processing', 'Processing'),
        ('success', 'Completed'),
        ('fail', 'Failed')]
    input_file = models.CharField(max_length=1024, blank=False, null=False,
        help_text='The file that was captured by inotfywait and made it through the filter.')
    priority = models.PositiveIntegerField(blank=False, null=False,
        help_text='The priority of this job: 0 = highest priority.')
    added = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)
    configuration = models.ForeignKey(Configuration, blank=False, null=False)
    status = models.CharField(max_length=30, choices=JOB_QUEUE_STATUS_CHOICES)

    def __unicode__(self):
        return self.input_file + ': ' + self.status

class InotifyWait:
    INOTIFYWAIT_CMD = 'inotifywait'
    INOTIFYWAIT_ARGS = '--quiet --monitor --event CLOSE_WRITE --event MOVED_TO --format %w%f'
    #INOTIFYWAIT_CMD = 'echo'
    #INOTIFYWAIT_ARGS = '-q -m -e CLOSE_WRITE -f %f\ %w'
    INOTIFYWAIT_READLINE_TIMEOUT = 0.1

    def __init__(self, configuration=None):
        # configuration
        if configuration:
            self.configuration = configuration
        else:
            return None
        # Start 'er up...
        self.start()

    def start(self):
        popen_args_tuple = (self.INOTIFYWAIT_CMD,)
        popen_args_tuple += tuple(self.INOTIFYWAIT_ARGS.split())
        if self.configuration.filter:
            # ^.*\/\..*$ - This might be greedy and pick a hidden directory instead.
            # ^.*\/\.[^\/]*$ - Ensure we are operating on the file part of the input.
            popen_args_tuple += ('--exclude', self.configuration.filter)
        popen_args_tuple += tuple(self.configuration.watch_dir.split(', '))
        #print popen_args_tuple
        self.process = subprocess.Popen(popen_args_tuple, stdout=subprocess.PIPE)

    def stop(self):
        self.process.terminate()

    def readline(self):
        # Two options here to stop the function blocking:
        # 1. Use signal.alarm and register a dummy function
        # 2. Use select.select with a timeout
        (rlist, wlist, xlist) = select.select([self.process.stdout], [], [],
            self.INOTIFYWAIT_READLINE_TIMEOUT)
        if rlist:
            #return self.process.stdout.readline()
            return rlist[0].readline()
        return ''

class Processor(dict):
    # 0 when idle, PID otherwise
    CPU_IDLE_FLAG = None
    def __init__(self, **kwargs):
        self.__setitem__(0, self.CPU_IDLE_FLAG)
        if kwargs.has_key('cpus'):
            for i in range(kwargs['cpus']):
                self.__setitem__(i, self.CPU_IDLE_FLAG)

    def active(self):
        for cpu_status in self.values():
            return cpu_status != self.CPU_IDLE_FLAG
        return False

    def available(self):
        self.check_jobs()
        for cpu in self.keys():
            cpu_status = self.get(cpu)
            if cpu_status == self.CPU_IDLE_FLAG:
                return True
        return False

    def check_jobs(self):
        for cpu in self.keys():
            cpu_status = self.get(cpu)
            if cpu_status == self.CPU_IDLE_FLAG:
                # No process on this CPU
                continue
            elif type(cpu_status['process'].poll()) == type(None):
                # Process is still running
                continue
            elif cpu_status['process'].poll() == 0:
                # The job completed successfully
                cpu_status['job'].status = 'success'
                cpu_status['job'].save()
                self.__setitem__(cpu, self.CPU_IDLE_FLAG)
            else:
                # Failure of some sort
                # Update job to Failed status and save
                cpu_status['job'].status = 'fail'
                cpu_status['job'].save()
                self.__setitem__(cpu, self.CPU_IDLE_FLAG)

    def start_job(self, job):
        # Find the first unused CPU
        for cpu, pid in self.items():
            if pid == self.CPU_IDLE_FLAG:
                break
        # cpu is the processor to use
        popen_args_tuple = tuple(job.configuration.run_script.split(' '))
        popen_args_tuple += (job.input_file,)
        #print popen_args_tuple
        try:
            job.status = 'processing'
            self.__setitem__(cpu, {
                'job': job,
                'process': subprocess.Popen(popen_args_tuple)})
        except:
            job.status = 'fail'
        else:
            mask = 2**(cpu)
            affinity.set_process_affinity_mask(self.__getitem__(cpu)['process'].pid, mask)
        finally:
            job.save()

