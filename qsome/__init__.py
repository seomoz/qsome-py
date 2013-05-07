'''The qsome base class'''

import qless
import pkgutil
import simplejson as json

# Internal imports
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
        return [Job(self.client, **job) for job in json.loads(
            self.client('job.get', *jids))]

    def tracked(self):
        '''Return an array of job objects that are being tracked'''
        results = json.loads(self.client('tracked'))
        results['jobs'] = [Job(self, **job) for job in results['jobs']]
        return results

    def failed(self, group=None, start=0, limit=25):
        '''If no group is provided, this returns a JSON blob of the counts of
        the various types of failures known. If a type is provided, returns
        paginated job objects affected by that kind of failure.'''
        if not group:
            return json.loads(self.client('failed'))
        else:
            results = json.loads(
                self.client('failed', group, start, limit))
            results['jobs'] = [Job(self.client, **j) for j in results['jobs']]
            return results

    def __getitem__(self, jid):
        '''Get a job object corresponding to that jid, or ``None`` if it
        doesn't exist'''
        results = self.client('get', jid)
        if not results:
            results = json.loads(
                self.client('recur.get', jid))
            if not results:
                return None
            return RecurringJob(self.client, **results)
        return Job(self.client, **json.loads(results))


class Events(qless.Events):
    '''Access to qsome events'''
    pass


class Client(qless.client):
    '''Access to the qsome api'''
    def __init__(self, *args, **kwargs):
        qless.client.__init__(self, *args, **kwargs)
        # Access to our queues
        self.queues = Queues(self)
        self.jobs = Jobs(self)
        # We now have a single unified core script.
        data = pkgutil.get_data('qsome', 'qsome-core/qsome.lua')
        self._lua = self.redis.register_script(data)


from .job import Job, RecurringJob
# There are a few classes that need no modification from the original qless
# versions. As such, we'll be importing those directly into this namespace
# so as to appear part of qsome.
from .config import Config
from .queue import Queue
