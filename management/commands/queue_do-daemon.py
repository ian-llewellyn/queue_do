from queue_do.models import Configuration, JobQueue, InotifyWait, Processor
#import sys # exit
import time # sleep

CPUS = 4
MAIN_LOOP_SLEEP_TIME = 1

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
