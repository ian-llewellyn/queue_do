from queue_do.models import Configuration, JobQueue
import sys # exit
import subprocess # call
import time # sleep
import select # select

CPUS = 4
MAIN_LOOP_SLEEP_TIME = 1

inotifywait_cmd = 'inotifywait'
inotifywait_args = '--quiet --monitor --event CLOSE_WRITE --event MOVED_TO --format %w%f'
#inotifywait_cmd = 'echo'
#inotifywait_args = '-q -m -e CLOSE_WRITE -f %f\ %w'

class InotifyWait:
    INOTIFYWAIT_READLINE_TIMEOUT = 0.1

    def __init__(self, configuration=None):
        # args may be a string: 'string',
        # ... many strings: 'a', 'b', 'c',
        # ... a list of strings: ['a', 'b', 'c']
        # ... or a combination of these.
        #self.watch_dirs = []
        #for arg in args:
        #    if type(arg) == list:
        #        self.watch_dirs += arg
        #    if type(arg) == str:
        #        self.watch_dirs.append(arg)

        # filter
        #if kwargs.has_key('filter'):
        #    self.filter = kwargs['filter']

        # configuration
        if configuration:
            self.configuration = configuration
        else:
            return None
            #self.filter = self.configuration.filter
            #self.watch_dir = self.configuration.watch_dir
            #self.run_script = self.configuration.run_script
            #self.default_priority = self.configuration.default_priority

        # Start 'er up...
        self.start()

    def start(self):
        popen_args_tuple = (inotifywait_cmd,)
        popen_args_tuple += tuple(inotifywait_args.split())
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

    def available(self):
        for cpu in self.keys():
            if self[cpu] == self.CPU_IDLE_FLAG:
                return True
            elif not self[cpu]['process'].poll():
                # Process is still running
                continue
            elif self[cpu]['process'].poll() == 0:
                # The job completed successfully
                # Update job to Success status and save
                self[cpu]['job'].status = 'success'
                self[cpu]['job'].save()
                self[cpu] = self.CPU_IDLE_FLAG
                return True
            else:
                # Failure of some sort
                # Update job to Failed status and save
                self[cpu]['job'].status = 'fail'
                self[cpu]['job'].save()
                self[cpu] = self.CPU_IDLE_FLAG
                return True
        return False

    def start_job(self, job):
        # Find the first unused CPU
        for cpu, pid in self.items():
            if pid == self.CPU_IDLE_FLAG:
                break
        # cpu is the processor to use
        popen_args_tuple = tuple(job.configuration.run_script)
        popen_args_tuple += (job.input_file,)
        try:
            job.status = 'processing'
            self.__setitem__(cpu, {'job': job,
                'process': subprocess.Popen(popen_args_tuple)})
        except:
            job.status = 'fail'
        finally:
            job.save()

## Setup the processor flags:
processor = Processor(cpus=CPUS)

## Read configuration from DB
inotifys = []
for configuration in Configuration.objects.all():
    inotifys.append(InotifyWait(configuration=configuration))

## Launch inotifywait
#for inotify in inotifys:
#    inotify.start()

running = True
## MAIN LOOP
while running:
    ## QUQUE INCOMING FILES - Read input (if any) from inotify instances
    for inotify in inotifys:
        #print 'Checking inotifywait with PID: %s' % inotify.process.pid
        end_of_file = False
        while not end_of_file:
            msg = inotify.readline().strip()
            #print '[%s]' % msg
            if msg == '':
                end_of_file = True
                break
            jq = JobQueue()
            jq.input_file = msg
            jq.priority = inotify.configuration.default_priority
            jq.configuration = inotify.configuration
            jq.status = 'queued'
            jq.save()


    ## PROCESS JOBS IN QUEUE
    status_sel = ['queued']
    # FIXME: Check to see if priority is the dominating ORDER BY - that is what we want.
    jobs = JobQueue.objects.filter(status__in = status_sel) \
        .order_by('added') \
        .order_by('priority')

    for job in jobs:
        if processor.available():
            # Start the job
            processor.start_job(job)
        else:
            # No processors are available
            break
    # All CPUs in use or all jobs started

    time.sleep(MAIN_LOOP_SLEEP_TIME)
