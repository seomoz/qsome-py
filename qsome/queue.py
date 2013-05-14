'''Our queue manipulations'''

import time
import uuid
import qless
import simplejson as json

from .job import Job


class Queue(qless.Queue):
    '''For most intents and purposes, this class is identical to the one
    provided by qless. The major differences are that jobs must also include
    hashes used to split them up into subqueues. The overridden functions
    reflect that need'''
    def put(self, klass, hsh, data, priority=None, tags=None, delay=None,
        retries=None, jid=None, depends=None):
        '''Either create a new job in the provided queue with the provided
        attributes, or move that job into that queue. If the job is being
        serviced by a worker, subsequent attempts by that worker to either
        `heartbeat` or `complete` the job should fail and return `false`.

        The `priority` argument should be negative to be run sooner rather
        than later, and positive if it's less important. The `tags` argument
        should be a JSON array of the tags associated with the instance and
        the `valid after` argument should be in how many seconds the instance
        should be considered actionable.'''
        return self.client('queue.put', self.name,
            jid or uuid.uuid4().hex,
            self.class_string(klass),
            hsh,
            json.dumps(data),
            delay or 0,
            'priority', priority or 0,
            'tags', json.dumps(tags or []),
            'retries', retries or 5,
            'depends', json.dumps(depends or [])
        )

    def recur(self, klass, hsh, data, interval, offset=0, priority=None,
        tags=None, retries=None, jid=None):
        '''Place a recurring job in this queue'''
        return self.client('queue.recur', self.name,
            jid or uuid.uuid4().hex,
            self.class_string(klass),
            hsh,
            json.dumps(data),
            'interval', interval, offset,
            'priority', priority or 0,
            'tags', json.dumps(tags or []),
            'retries', retries or 5
        )

    def pop(self, count=None):
        '''Passing in the queue from which to pull items, the current time,
        when the locks for these returned items should expire, and the number
        of items to be popped off.'''
        results = [Job(self.client, **job) for job in json.loads(
            self.client('queue.pop', self.name, self.worker_name, count or 1))]
        if count == None:
            return (len(results) and results[0]) or None
        return results

    def peek(self, count=None):
        '''Similar to the pop command, except that it merely peeks at the next
        items'''
        results = [Job(self.client, **rec) for rec in json.loads(
            self.client('queue.peek', self.name, count or 1))]
        if count == None:
            return (len(results) and results[0]) or None
        return results

    def subqueues(self):
        '''Return the names of the various subqueues of which this queue is
        comprised'''
        # Because empty arrays in lua come through as dictionaries, we have to
        # force this to be a lisit if it's empty
        return json.loads(self.client('queue.subqueues', self.name)) or []

    def resize(self, size):
        '''Resize this queue to contain `size` subqueues'''
        return self.client('queue.resize', self.name, size)

    def config(self, key=None, val=None):
        '''Get, set configuration options'''
        if key == None:
            return json.loads(self.client('queue.config', self.name))
        elif val == None:
            return json.loads(self.client('queue.config', self.name, key))
        else:
            return json.loads(self.client('queue.config', self.name, key, val))

    def __len__(self):
        return self.client('queue.length', self.name)

    def stats(self, date=None):
        '''Get some stats about the wait and run times of jobs in the queue'''
        return json.loads(
            self.client('queue.stats', self.name, date or str(time.time())))
