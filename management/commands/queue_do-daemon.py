from queue_do.models import Configuration, JobQueue, InotifyWait, Processor
import sys # exit
import time # sleep
import signal # signal, SIGTERM, SIGINT, SIGUSR2

CPUS = 4
MAIN_LOOP_SLEEP_TIME = 1

## Signal Handling Functions
def terminate(a, b):
    # - send SIGTERM to all child processes
    for inotify in inotifys:
        inotify.stop()
    for cpu in processor:
        cpu_status = processor[cpu]
        if cpu_status == processor.CPU_IDLE_FLAG:
            continue
        cpu_status['process'].terminate()
        # - set job status to fail
        cpu_status['job'].status = 'fail'
        cpu_status['job'].save()
    # - exit
    sys.exit(1)

def finish_current(a, b):
    # - send SIGTERM to all InotfiyWait processes (no more queueing)
    for inotify in inotifys:
        inotify.stop()
    # - wait until active jobs finish
    print 'processor.active():', processor.active()
    while processor.active():
        print 'Processor still active'
        processor.check_jobs()
        time.sleep(MAIN_LOOP_SLEEP_TIME)
    print 'processor.active():', processor.active()
    # - exit
    sys.exit(0)

## Register Signal Handlers
# SIGTERM:
signal.signal(signal.SIGTERM, terminate)
# Ctrl-C equivelant to SIGTERM
signal.signal(signal.SIGINT, terminate)
signal.signal(signal.SIGINT, finish_current)
# SIGUSR2:
signal.signal(signal.SIGUSR2, finish_current)

## Setup the processor flags:
processor = Processor(cpus=CPUS)

## Read configuration from DB
inotifys = []
for configuration in Configuration.objects.all():
    inotifys.append(InotifyWait(configuration=configuration))

jobs = True	# This will skip the DB query optimisation on the first run.
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
            # This ensures that we go on to the DB SELECT query
            jobs = True


    # We could save a lot of DB SELECT queries here by continueing
    # here in the event that all jobs have been completed,
    # and no more files have been received through inotify.
    if not jobs:
        if processor.active():
            processor.check_jobs()
        continue
    # The downside is that putting a job in the queue from the Django admin
    # interface will have no effect until a job is queued via this daemon.


    ## PROCESS JOBS IN QUEUE
    status_sel = ['queued']
    # FIXME: Check to see if priority is the dominating ORDER BY - that is what we want.
    jobs = JobQueue.objects.filter(status__in = status_sel) \
        .order_by('-status') \
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
