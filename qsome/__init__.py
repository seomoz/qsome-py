'''The qsome base class'''

import qless
import pkgutil
from qless import logger
from qless import Jobs, Queues, Events


class Queues(qless.Queues):
    '''Access to some global queues information'''
    def __getitem__(self, queue_name):
        '''Get a queue object associated with the provided queue name'''
        return Queue(queue_name, self.client, self.client.worker_name)


class Jobs(qless.Jobs):
    '''Access to some global jobs information'''
    def __call__(self, *jids):
        '''Retun job objects for each of these jids'''
        return [Job(self.client, **kwargs) for job in json.loads(
            self.client('job.get', *jids))]


class Events(qless.Events):
    '''Access to qsome events'''
    pass


class Client(qless.client):
    '''Access to the qsome api'''
    def __init__(self, *args, **kwargs):
        qless.client.__init__(self, *args, **kwargs)
        # Access to our queues
        self.queues = Queues(self)
        # We now have a single unified core script.
        data = pkgutil.get_data('qsome', 'qsome-core/qsome.lua')
        self._lua = self.redis.register_script(data)


from .job import Job, RecurringJob
# There are a few classes that need no modification from the original qless
# versions. As such, we'll be importing those directly into this namespace
# so as to appear part of qsome.
from .config import Config
from .queue import Queue
