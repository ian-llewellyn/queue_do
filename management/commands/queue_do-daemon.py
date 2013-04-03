from queue_do.models import Configuration, JobQueue
import sys # exit
import subprocess # call

#inotifywait_cmd = 'inotifywait'
#inotifywait_args = '-m -e CLOSE_WRITE -e MOVE_TO'
inotifywait_cmd = 'echo'
inotifywait_args = ''

class InotifyWait:
    def __init__(self, *args, **kwargs):
        # args may be a string: 'string',
        # ... many strings: 'a', 'b', 'c',
        # ... a list of strings: ['a', 'b', 'c']
        # ... or a combination of these.
        self.watch_dirs = []
        for arg in args:
            if type(arg) == list:
                self.watch_dirs += arg
            if type(arg) == str:
                self.watch_dirs.append(arg)

        for arg in kwargs:
            # filter
            if kwargs.has_key('filter'):
                self.filter = kwargs['filter']
            # configuration
            if kwargs.has_key('configuration'):
                self.configuration = kwargs['configuration']

        # Start 'er up...
        self.start()

    def start(self):
        popen_args_tuple = (inotifywait_cmd,)
        popen_args_tuple += tuple(inotifywait_args.split())
        popen_args_tuple += tuple(self.watch_dirs)
        # popen_args_tuple += self.watch_dirs
        self.process = subprocess.Popen(popen_args_tuple), stdout=subprocess.PIPE)

## Read configuration from DB
inotifys = []
for configuration in Configuration.object.all():
    inotifys.append(InotifyWait(configuration.watch_dir, configuration=configuration))

## Launch inotifywait | queue_do-add.py
for inotify in inotifys:
    inotify.start()

for inotify in inotifys:
    end_of_file = False
    while not end_of_file:
        msg = inotify.readline().strip()
        if msg == '':
            end_of_file = True
            break
        jq = JobQueue()
        jq.input_file = msg
        jq.priority = inotfiy.configuration.default_priority
        jq.status = 'queued'
        jq.save()


status_sel = ['queued']
jobs = JobQueue.objects.filter(status__in = status_sel) \
         .order_by('added') \
         .order_by('priority')

for job in jobs:
    if availability:
        job.start()








