from django.core.management.base import BaseCommand
from queue_do.single_instance import SingleInstance
from queue_do.daemon import Daemon
from optparse import make_option

from queue_do.models import JobQueue

import sys # exit
import time # sleep
import signal # signal, SIGTERM, SIGINT, SIGUSR2
import logging

logger = logging.getLogger('queue_do.purge_queue')

class Command(BaseCommand):
    help = """
    This command purges jobs from the queue that are more than <days> old.
    """

    option_list = BaseCommand.option_list + (
        make_option('--keep-days',
            action='store',
            dest='keep_days',
            default="30",
            help='Number of days to keep records for (default 30)'),
        make_option('--remove-failed',
            action='store_true',
            dest='remove_failed',
            default=False,
            help='Clear failed jobs from the queue also'),
        )

    def handle(self, *args, **kwargs):
        keep_days = int(kwargs['keep_days'])

        logger.info('Starting Queue_Do Purge')
        JobQueue.PurgeQueue(keep_days, kwargs['remove_failed'])
        logger.info('Finished purging Queue_Do jobs')
