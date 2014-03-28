from queue_do.models import Configuration, JobQueue, InotifyWait, Processor
from django.core.management.base import BaseCommand

from queue_do.single_instance import SingleInstance

import sys # exit
import time # sleep
import signal # signal, SIGTERM, SIGINT, SIGUSR2

import logging

CPUS = 4
MAIN_LOOP_SLEEP_TIME = 1
DB_CHECK_INTERVAL = 60
RETRIES = 3

logger = logging.getLogger('queue_do.daemon')

class Daemon(object):
    ## Signal Handling Functions
    @staticmethod
    def _terminate(a, b):
        for cpu in processor:
            cpu_status = processor[cpu]
            if cpu_status == processor.CPU_IDLE_FLAG:
                # This CPU is idle, move on to the next one...
                continue
            # Set job status to fail
            cpu_status['job'].status = 'fail'
            cpu_status['job'].save()
        # exit (unnaturally)
        sys.exit(1)
    
    @staticmethod
    def _finish_current(a, b):
        # Wait until active jobs finish
        while processor.active():
            processor.check_jobs()
            time.sleep(MAIN_LOOP_SLEEP_TIME)
        # exit
        sys.exit(0)

    def start(self, retry_failed):
        ## Read configuration from DB and start inotify watchers
        self.inotifys = []
        for configuration in Configuration.objects.all():
            # Ensure inotifys ignore SIGUSR1 - we use this for status
            sigusr1_handler = signal.signal(signal.SIGUSR1, signal.SIG_IGN)
            # Instantiate InotifyWait
            self.inotifys.append(InotifyWait(configuration=configuration))

        ## Register Signal Handlers
        # SIGTERM:
        signal.signal(signal.SIGTERM, self._terminate)
        # Ctrl-C equivelant to SIGTERM
        signal.signal(signal.SIGINT, self._terminate)
        # SIGUSR2:
        signal.signal(signal.SIGUSR2, self._finish_current)

        ## Setup the processor flags:
        self.processor = Processor(cpus=CPUS)

        ## Timer to reduce frequency of database SELECTs
        last_db_check = 0

        # Ensure we check the DB on the first run.
        check_db = True

        # Initiate an empty jobs array
        jobs = []

        ## MAIN LOOP
        while True:
            ## QUQUE INCOMING FILES - Read input (if any) from inotify instances
            for inotify in self.inotifys:
                logger.debug('Checking inotifywait with PID: %s' % inotify.process.pid)
                while True:
                    msg = inotify.readline().strip()
                    logger.debug('inotifywait output: %s' % msg)
                    if msg == '':
                        # No message read from inotify - no job to queue
                        logger.debug('No message read from inotify - no job to queue')
                        break
                    jq = JobQueue()
                    jq.input_file = msg
                    jq.priority = inotify.configuration.default_priority
                    jq.configuration = inotify.configuration
                    jq.status = 'queued'
                    jq.save()
                    logger.info('Added Job Queue: %s' % jq)
                    check_db = True	# This ensures that we go on to the DB SELECT query


            ## DATABASE BIT
            if time.time() >= (last_db_check + DB_CHECK_INTERVAL):
                # If DB_CHECK_INTERVAL has elapsed, we will want to do a DB SELECT.
                check_db = True

            # Database query
            if check_db:
                status_sel = ['queued']
                if retry_failed:
                    status_sel.append('fail')
                jobs = JobQueue.objects.filter(status__in=status_sel) \
                    .exclude(status='fail', failure_count__gt=RETRIES) \
                    .order_by('-status') \
                    .order_by('added') \
                    .order_by('priority')
                # Reset check_db in order to wait DB_CHECK_INTERVAL seconds
                last_db_check = time.time()
                check_db = False


            ## HOUSEKEEPING
            if self.processor.active():
                # Check previously running jobs, and reset the CPU to available.
                self.processor.check_jobs()


            ## PROCESS JOBS IN QUEUE
            # convert to a list
            jobs = [job for job in jobs]

            # Process each item
            while len(jobs) > 0:
                if not self.processor.available():
                    # No processors are available
                    logger.info('No processors are available, waiting...')
                    break

                # Pop the job to be processed
                job = jobs.pop(0)

                # Stop certain signals from getting to children
                sigusr1_handler = signal.signal(signal.SIGUSR1, signal.SIG_IGN)
                sigusr2_handler = signal.signal(signal.SIGUSR2, signal.SIG_IGN)
                # Start the job
                self.processor.start_job(job)
                logger.info('Started job: %s' % job)
                # Reset signal handlers
                signal.signal(signal.SIGUSR1, sigusr1_handler)
                signal.signal(signal.SIGUSR2, sigusr2_handler)


            # All CPUs in use, all jobs started or no jobs to process
            logger.debug('Sleeping for %s seconds.' % MAIN_LOOP_SLEEP_TIME)
            time.sleep(MAIN_LOOP_SLEEP_TIME)
