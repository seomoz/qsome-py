'''Our job classes'''

from . import logger

import qless
import simplejson as json


class Job(qless.Job):
    '''A job representing a unit of work'''
    def __init__(self, client, **kwargs):
        qless.Job.__init__(self, client, **kwargs)
        object.__setattr__(self, 'hash', int(kwargs['hash']))

    def move(self, queue, delay=0, depends=None):
        '''Move this job out of its existing state and into another queue. If
        a worker has been given this job, then that worker's attempts to
        heartbeat that job will fail. Like ``Queue.put``, this accepts a
        delay, and dependencies'''
        logger.info('Moving %s to %s from %s' % (
            self.jid, queue, self.queue_name))
        return self.client('put', queue, self.jid, self.klass_name, self.hash,
            json.dumps(self.data), delay, 'depends', json.dumps(depends or [])
        )


class RecurringJob(qless.RecurringJob):
    '''An object representing a recurring job'''
    def __init__(self, client, **kwargs):
        qless.RecurringJob.__init__(self, client, **kwargs)
        object.__setattr__(self, 'hash', kwargs['hash'])
