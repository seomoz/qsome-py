'''Our job classes'''

from . import logger

import qless
import simplejson as json
from qsome.exceptions import QlessException, LostLockException


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
        return self.client('queue.put', queue, self.jid, self.klass_name,
            self.hash, json.dumps(self.data), delay, 'depends',
            json.dumps(depends or [])
        )

    def heartbeat(self):
        '''Renew the heartbeat, if possible, and optionally update the job's
        user data.'''
        logger.debug('Heartbeating %s (ttl = %s)' % (self.jid, self.ttl))
        try:
            self.expires_at = float(self.client('job.heartbeat', self.jid,
            self.client.worker_name, json.dumps(self.data)) or 0)
        except QlessException as exc:
            logger.exception('Lock lost for %s' % self.jid)
            raise LostLockException(self.jid)
        logger.debug('Heartbeated %s (ttl = %s)' % (self.jid, self.ttl))
        return self.expires_at

    def track(self):
        '''Start tracking this job'''
        return json.loads(self.client('job.track', 'track', self.jid))

    def untrack(self):
        '''Stop tracking this job'''
        return json.loads(self.client('job.track', 'untrack', self.jid))


class RecurringJob(qless.RecurringJob):
    '''An object representing a recurring job'''
    def __init__(self, client, **kwargs):
        qless.RecurringJob.__init__(self, client, **kwargs)
        object.__setattr__(self, 'hash', kwargs['hash'])
