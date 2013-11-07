from django.core.management.base import BaseCommand
from queue_do.single_instance import SingleInstance
from queue_do.daemon import Daemon
from optparse import make_option

from queue_do.models import Configuration, JobQueue, InotifyWait, Processor

import sys # exit
import time # sleep
import signal # signal, SIGTERM, SIGINT, SIGUSR2
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand, SingleInstance):
    help = """
    This daemon runs an inotifywait on each configured diectory. For each file
    that is detected, a job is added to the Job Queue from where a separate
    thread picks it up and runs the pre-configured arbitrary program.

    --retry-failed: flag to force retry of failed items
    --progid NAME: optional name to give the lockfile
    """

    option_list = BaseCommand.option_list + (
        make_option('--progid',
            action='store',
            dest='progid',
            default="queue_do-daemon",
            help='Programme name for lock file'),
        make_option('--retry-failed',
            action='store_true',
            dest='retry_failed',
            default=False,
            help='Retry failed items in queue'),
        )

    def handle(self, *args, **kwargs):
        self.SingleInstance(kwargs['progid'])

        logger.info('Queue_Do Daemon started')
        daemon = Daemon()
        daemon.start(retry_failed=kwargs['retry_failed'])
        logger.info('Queue_Do Daemon ended')

