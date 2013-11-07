"""
    SingleInstance

    This is intended for use by django commands - to make sure that only one
    instance runs. It creates a pid file in /tmp and if one exists for
    a running process, it exits.


    Example ...

    import sys
    from reaper.single_instance import SingleInstance
    from django.core.management.base import BaseCommand

    class Command(BaseCommand, SingleInstance):
        help = "Demo Django Command"

        def handle(self, *args, **options):
            self.SingleInstance(sys.argv[1])

"""

import os
import sys
import atexit
import logging

logger = logging.getLogger(__name__)

class SingleInstance(object):

    def SingleInstance(self, program_name=None):
        """Ensures that only one instance of the program is running."""
        pid = str(os.getpid())
        if program_name:
            pidfile = "/tmp/%s.pid" % program_name
        else:
            pidfile = "/tmp/%s.pid" % sys.argv[1]

        if os.path.isfile(pidfile):
            oldpid = file(pidfile, 'r').read()
            already_running = True
            try:
                os.kill(int(oldpid), 0)
            except:
                already_running = False
                logger.warn("Warning, %s exists, but pid %s not running" % (pidfile, oldpid))
                os.unlink(pidfile)
                file(pidfile, 'w').write(pid)
            if already_running:
                logger.debug("%s already exists, running as pid %s, exiting" % (pidfile, oldpid))
                sys.exit(0)
        else:
            file(pidfile, 'w').write(pid)

        atexit.register(lambda: os.unlink(pidfile))

