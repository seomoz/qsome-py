#! /usr/bin/env python

'''Some basic sanity checks'''

import time
from common import TestQsome

from qsome.exceptions import QlessException, LostLockException


class TestBasic(TestQsome):
    '''Some basic sanity checks'''
    def test_put_pop(self):
        '''Let's make sure that we can at the very least put and pop jobs'''
        # We'll put a single job in the queue, and verify that we get the same
        # job back
        jid = self.queue.put('Foo', 15, {'test': 'test_put_pop'})
        job = self.queue.pop()
        self.assertEqual(job.jid, jid)
        # We also need to make sure that we get the same hash value back
        self.assertEqual(job.hash, 15)
        # After we've popped the only job off the queue, we should not see
        # another job come out
        self.assertEqual(self.queue.pop(), None)

    def test_config(self):
        '''Make sure we can manipulate the congiruation'''
        # Set this particular configuration value
        config = self.client.config
        config['testing'] = 'foo'
        self.assertEqual(config['testing'], 'foo')
        # Now let's get all the configuration options and make
        # sure that it's a dictionary, and that it has a key for 'testing'
        self.assertTrue(isinstance(config.all, dict))
        self.assertEqual(config.all['testing'], 'foo')
        # Now we'll delete this configuration option and make sure that
        # when we try to get it again, it doesn't exist
        del config['testing']
        self.assertEqual(config['testing'], None)
        self.assertRaises(AttributeError, config.__getattr__, 'foo')

    def test_put_get(self):
        '''We should be able to put a job in a queue, get it and delete it'''
        jid = self.queue.put('Foo', 0, {'test': 'put_get'})
        job = self.client.jobs[jid]
        self.assertEqual(job.priority, 0)
        self.assertEqual(job.hash, 0)
        self.assertEqual(job.data, {'test': 'put_get'})
        self.assertEqual(job.tags, [])
        self.assertEqual(job.worker_name, '')
        self.assertEqual(job.state, 'waiting')
        self.assertEqual(job.klass_name, 'Foo')

        # Now cancel it and make sure we can't find it now
        job.cancel()
        self.assertEqual(self.client.jobs[jid], None)

    def test_put_pop_attributes(self):
        '''When we pop a job, we should see all the attributes we expect'''
        self.queue.put('Foo', 0, {'test': 'test_put_pop_attributes'})
        job = self.queue.pop()
        self.assertEqual(job.worker_name, self.client.worker_name)
        self.assertEqual(job.state, 'running')
        self.assertEqual(job.retries_left, 5)
        self.assertTrue(job.ttl > 0)

    def test_data_access(self):
        '''Ensure job data is accessible through []'''
        job = self.client.jobs[
            self.queue.put('Foo', 0, {'test': 'data_access'})]
        self.assertEqual(job['test'], 'data_access')

    def test_put_pop_priority(self):
        '''Ensure priority works to actually give jobs higher priority'''
        jids = [self.queue.put('Foo', 0, {
            'test': 'put_pop_priority', 'count': c
        }, priority=c) for c in range(10)]
        last = len(jids)
        for _ in range(len(jids)):
            job = self.queue.pop()
            self.assertTrue(job['count'] < last)
            job.complete()
            last = job['count']

    def test_scheduled(self):
        '''Ensure scheduled jobs aren't available until they're ripe'''
        time.freeze()
        jid = self.queue.put('Foo', 0, {'test': 'scheduled'}, delay=10)
        self.assertEqual(self.queue.pop(), None)
        time.advance(11)
        job = self.queue.pop()
        self.assertNotEqual(job, None)
        self.assertEqual(job.jid, jid)
        time.thaw()

    def test_move_queue(self):
        '''When we move a queue, ensure it /really/ moves'''
        jid = self.queue.put('Foo', 0, {'test': 'move_queue'})
        self.assertEqual(len(self.queue), 1)
        self.assertEqual(len(self.other), 0)
        self.client.jobs[jid].move('other')
        self.assertEqual(len(self.queue), 0)
        self.assertEqual(len(self.other), 1)

    def test_move_queue_popped(self):
        '''When a running job is moved, heartbeats should fail'''
        self.queue.put('Foo', 0, {'test': 'move_queue_popped'})
        self.assertEqual(len(self.queue), 1)
        job = self.queue.pop()
        self.assertNotEqual(job, None)
        # Now move it
        job.move('other')
        self.assertRaises(LostLockException, job.heartbeat)

    def test_move_non_destructive(self):
        '''Moving a job shouldn't change its attributes'''
        jid = self.queue.put('Foo', 0, {
            'test': 'move_non_destructive'
        }, tags=['foo', 'bar'], priority=5)
        before = self.client.jobs[jid]
        before.move('other')
        after = self.client.jobs[jid]
        self.assertEqual(before.tags, ['foo', 'bar'])
        self.assertEqual(before.priority, 5)
        self.assertEqual(before.tags, after.tags)
        self.assertEqual(before.data, after.data)
        self.assertEqual(before.priority, after.priority)

    def test_heartbeat(self):
        '''Heartbeating a job should keep its lock'''
        self.assertEqual(len(self.worker_a), 0, 'Start with an empty queue')
        self.queue.put('Foo', 0, {'test': 'heartbeat'})
        ajob = self.worker_a.pop()
        self.assertNotEqual(ajob, None)
        bjob = self.worker_b.pop()
        self.assertEqual(bjob, None)
        self.assertTrue(isinstance(ajob.heartbeat(), float))
        self.assertTrue(ajob.ttl > 0)
        # Now try setting a heartbeat
        self.client.config['heartbeat'] = -60
        self.assertTrue(isinstance(ajob.heartbeat(), float))
        self.assertTrue(ajob.ttl <= 0)

    def test_heartbeat_expiration(self):
        '''Heartbeating should update the job's expiration'''
        self.client.config['crawl-heartbeat'] = 7200
        self.queue.put('Foo', 0, {})
        job = self.worker_a.pop()
        self.assertEqual(self.worker_b.pop(), None)
        time.freeze()
        # Now, we'll advance the apparent system clock, and heartbeat
        for _ in range(10):
            time.advance(3600)
            self.assertNotEqual(job.heartbeat(), False)
            self.assertEqual(self.worker_b.pop(), None)

        # Reset it to the original time object
        time.thaw()

    def test_heartbeat_state(self):
        '''Only popped jobs should be heartbeatable'''
        self.assertEqual(len(self.queue), 0, 'Start with an empty queue')
        jid = self.queue.put('Foo', 0, {'test': 'heartbeat_state'})
        job = self.client.jobs[jid]
        self.assertRaises(LostLockException, job.heartbeat)

    def test_peek_pop_empty(self):
        '''Pops from empty queues shouldn't return jobs'''
        self.assertEqual(self.queue.pop(), None)
        self.assertEqual(self.queue.peek(), None)

    def test_locks(self):
        '''Locks can be handed out to new workers'''
        self.queue.put('Foo', 0, {'test': 'locks'})
        # Reset our heartbeat for both A and B
        self.client.config['heartbeat'] = -10
        # Make sure a gets a job
        ajob = self.worker_a.pop()
        self.assertNotEqual(ajob, None)
        # Now, make sure that b gets that same job
        bjob = self.worker_b.pop()
        self.assertNotEqual(bjob, None)
        self.assertEqual(ajob.jid, bjob.jid)
        self.assertTrue(isinstance(bjob.heartbeat(), float))
        self.assertTrue((bjob.heartbeat() + 11) >= time.time())
        self.assertRaises(LostLockException, ajob.heartbeat)

    def test_locks_workers(self):
        '''When a worker loses a lock on a job, that job should be removed
        from the list of jobs owned by that worker'''
        self.queue.put('Foo', 0, {"test": "locks"}, retries=1)
        self.client.config["heartbeat"] = -10

        self.worker_a.pop()
        # Get the workers
        workers = dict((w['name'], w) for w in self.client.workers.counts)
        self.assertEqual(workers[self.worker_a.worker_name]["stalled"], 1)

        # Should have one more retry, so we should be good
        self.worker_b.pop()
        workers = dict((w['name'], w) for w in self.client.workers.counts)
        self.assertEqual(workers[self.worker_a.worker_name]["stalled"], 0)
        self.assertEqual(workers[self.worker_b.worker_name]["stalled"], 1)

        # Now it's automatically failed. Shouldn't appear in either worker
        self.worker_b.pop()
        workers = dict((w['name'], w) for w in self.client.workers.counts)
        self.assertEqual(workers[self.worker_a.worker_name]["stalled"], 0)
        self.assertEqual(workers[self.worker_b.worker_name]["stalled"], 0)

    def test_cancel(self):
        '''Canceled jobs can't be popped'''
        self.client.jobs[self.queue.put('Foo', 0, {'test': 'cancel'})].cancel()
        self.assertEqual(self.queue.pop(), None)

    def test_cancel_heartbeat(self):
        '''Canceled jobs can't be heartbeated, or completed'''
        self.queue.put('Foo', 0, {'test': 'cancel_heartbeat'})
        job = self.queue.pop()
        job.cancel()
        self.assertRaises(LostLockException, job.heartbeat)
        self.assertRaises(QlessException, job.complete)

    def test_cancel_fail(self):
        '''Canceled jobs aren't considered failed'''
        self.queue.put('Foo', 0, {'test': 'cancel_fail'})
        job = self.queue.pop()
        job.fail('foo', 'some message')
        self.assertEqual(self.client.jobs.failed(), {'foo': 1})
        job.cancel()
        self.assertEqual(self.client.jobs.failed(), {})

    def test_complete(self):
        '''Completed jobs should appear complete'''
        jid = self.queue.put('Foo', 0, {'test': 'complete'})
        job = self.queue.pop()
        self.assertNotEqual(job, None)
        self.assertEqual(job.complete(), 'complete')
        job = self.client.jobs[jid]
        self.assertEqual(job.state, 'complete')
        self.assertEqual(job.worker_name, '')
        self.assertEqual(job.queue_name, '')
        self.assertEqual(len(self.queue), 0)
        self.assertEqual(self.client.jobs.complete(), [jid])

        # Now, if we move job back into a queue, we shouldn't see any
        # completed jobs anymore
        job.move('testing')
        self.assertEqual(self.client.jobs.complete(), [])

    def test_complete_advance(self):
        '''Ensure we can simultaneously complete and advance a job'''
        jid = self.queue.put('Foo', 0, {'test': 'complete_advance'})
        job = self.queue.pop()
        self.assertNotEqual(job, None)
        self.assertEqual(job.complete('testing'), 'waiting')
        job = self.client.jobs[jid]
        self.assertEqual(len(job.history), 2)
        self.assertEqual(job.state, 'waiting')
        self.assertEqual(job.worker_name, '')
        self.assertEqual(job.queue_name, 'testing-1')
        self.assertEqual(len(self.queue), 1)

    def test_complete_fail(self):
        '''Only the worker holding a lock can complete a job'''
        jid = self.queue.put('Foo', 0, {'test': 'complete_fail'})
        self.client.config['heartbeat'] = -10
        ajob = self.worker_a.pop()
        self.assertNotEqual(ajob, None)
        bjob = self.worker_b.pop()
        self.assertNotEqual(bjob, None)
        self.assertRaises(QlessException, ajob.complete)
        self.assertEqual(bjob.complete(), 'complete')
        job = self.client.jobs[jid]
        self.assertEqual(job.state, 'complete')
        self.assertEqual(job.worker_name, '')
        self.assertEqual(job.queue_name, '')
        self.assertEqual(len(self.queue), 0)

    def test_complete_state(self):
        '''Waiting jobs cannot be completed'''
        jid = self.queue.put('Foo', 0, {'test': 'complete_fail'})
        job = self.client.jobs[jid]
        self.assertRaises(QlessException, job.complete, 'testing')

    def test_complete_queues(self):
        '''Next queues always appear in the list of queues'''
        self.queue.put('Foo', 0, {'test': 'complete_queues'})
        self.assertEqual(len(
            [q for q in self.client.queues.counts if q['name'] == 'other']), 0)
        self.queue.pop().complete('other')
        self.assertEqual(len(
            [q for q in self.client.queues.counts if q['name'] == 'other']), 1)

    def test_job_time_expiration(self):
        '''Colmpleted jobs eventually expire out'''
        self.client.config['jobs-history'] = -1
        jids = [self.queue.put('Foo', 0, {
            'test': 'job_time_experiation',
            'count': c
        }) for c in range(20)]
        for _ in range(len(jids)):
            self.queue.pop().complete()
        self.assertEqual(self.redis.zcard('ql:completed'), 0)
        self.assertEqual(len(self.redis.keys('ql:j:*')), 0)

    def test_job_count_expiration(self):
        '''Completed jobs eventually expire out'''
        self.client.config['jobs-history-count'] = 10
        jids = [self.queue.put('Foo', 0, {
            'test': 'job_count_expiration',
            'count': c
        }) for c in range(20)]
        for _ in range(len(jids)):
            self.queue.pop().complete()
        self.assertEqual(self.redis.zcard('ql:completed'), 10)
        self.assertEqual(len(self.redis.keys('ql:j:*')), 10)

    def test_stats_waiting(self):
        '''We should be able to get stats about how long jobs wait'''
        stats = self.queue.stats(time.time())
        self.assertEqual(stats['wait']['count'], 0)
        self.assertEqual(stats['run']['count'], 0)

        time.freeze()
        self.queue.resize(20)
        jids = [self.queue.put('Foo', c, {
            'test': 'stats_waiting',
            'count': c
        }) for c in range(20)]
        self.assertEqual(len(jids), 20)
        for _ in range(len(jids)):
            self.assertNotEqual(self.queue.pop(), None)
            time.advance(1)

        time.thaw()
        # Now, make sure that we see stats for the waiting
        stats = self.queue.stats(time.time())
        self.assertEqual(stats['wait']['count'], 20)
        self.assertEqual(stats['wait']['mean'], 9.5)
        # This is our expected standard deviation
        self.assertTrue(stats['wait']['std'] - 5.916079783099 < 1e-8)
        # Now make sure that our histogram looks like what we think it
        # should
        self.assertEqual(stats['wait']['histogram'][0:20], [1] * 20)
        self.assertEqual(sum(stats['run']['histogram']), stats['run']['count'])
        self.assertEqual(sum(stats['wait']['histogram']),
            stats['wait']['count'])

    def test_stats_complete(self):
        '''We should be able to get stats about how long jobs take to run'''
        stats = self.queue.stats(time.time())
        self.assertEqual(stats['wait']['count'], 0)
        self.assertEqual(stats['run']['count'], 0)

        time.freeze()
        self.queue.resize(20)
        jids = [self.queue.put('Foo', c, {
            'test': 'stats_waiting',
            'count': c
        }) for c in range(20)]
        jobs = self.queue.pop(20)
        self.assertEqual(len(jobs), 20)
        for job in jobs:
            job.complete()
            time.advance(1)

        time.thaw()
        # Now, make sure that we see stats for the waiting
        stats = self.queue.stats(time.time())
        self.assertEqual(stats['run']['count'], 20)
        self.assertEqual(stats['run']['mean'], 9.5)
        # This is our expected standard deviation
        self.assertTrue(stats['run']['std'] - 5.916079783099 < 1e-8)
        # Now make sure that our histogram looks like what we think it
        # should
        self.assertEqual(stats['run']['histogram'][0:20], [1] * 20)
        self.assertEqual(sum(stats['run']['histogram']), stats['run']['count'])
        self.assertEqual(sum(stats['wait']['histogram']),
            stats['wait']['count'])

    def test_queues(self):
        '''Queues can report the number of jobs they have in various states'''
        self.queue.resize(10)
        self.assertEqual(len(self.queue), 0)
        self.assertEqual(self.client.queues.counts, {})
        # Now, let's actually add an item to a queue, but scheduled
        self.queue.put('Foo', 0, {'test': 'queues'}, delay=10)
        expected = {
            'name': 'testing',
            'stalled': 0,
            'waiting': 0,
            'running': 0,
            'scheduled': 1,
            'depends': 0,
            'recurring': 0
        }
        self.assertEqual(self.client.queues.counts, [expected])
        self.assertEqual(self.client.queues["testing"].counts, expected)

        self.queue.put('Foo', 1, {'test': 'queues'})
        expected['waiting'] += 1
        self.assertEqual(self.client.queues.counts, [expected])
        self.assertEqual(self.client.queues["testing"].counts, expected)

        self.queue.pop()
        expected['waiting'] -= 1
        expected['running'] += 1
        self.assertEqual(self.client.queues.counts, [expected])
        self.assertEqual(self.client.queues["testing"].counts, expected)

        # Now we'll have to mess up our heartbeat to make this work
        self.queue.put('Foo', 2, {'test': 'queues'})
        self.client.config['heartbeat'] = -60
        self.queue.pop()
        expected['stalled'] += 1
        self.assertEqual(self.client.queues.counts, [expected])
        self.assertEqual(self.client.queues["testing"].counts, expected)

    def test_track(self):
        '''We should be able to track specific jobs'''
        self.assertEqual(
            self.client.jobs.tracked(), {'expired': {}, 'jobs': []})
        job = self.client.jobs[self.queue.put('Foo', 0, {'test':'track'})]
        job.track()
        self.assertEqual(len(self.client.jobs.tracked()['jobs']), 1)
        job.untrack()
        self.assertEqual(len(self.client.jobs.tracked()['jobs']), 0)
        job.track()
        job.cancel()
        self.assertEqual(len(self.client.jobs.tracked()['expired']), 1)

    def test_retries(self):
        '''Jobs honor their retry limits'''
        self.assertEqual(self.client.jobs.failed(), {})
        self.queue.put('Foo', 0, {'test': 'retries'}, retries=2)
        # Easier to lose the heartbeat lock
        self.client.config['heartbeat'] = -10
        self.assertNotEqual(self.queue.pop(), None)
        self.assertEqual(self.client.jobs.failed(), {})
        self.assertNotEqual(self.queue.pop(), None)
        self.assertEqual(self.client.jobs.failed(), {})
        self.assertNotEqual(self.queue.pop(), None)
        self.assertEqual(self.client.jobs.failed(), {})
        # This one should do it
        self.assertEqual(self.queue.pop(), None)
        self.assertEqual(self.client.jobs.failed(), {
            'failed-retries-testing-1': 1})

    def test_retries_complete(self):
        '''After completing, they get their original number of retries'''
        jid = self.queue.put('Foo', 0, {'test': 'retries_complete'}, retries=2)
        self.client.config['heartbeat'] = -10
        job = self.queue.pop()
        self.assertNotEqual(job, None)
        job = self.queue.pop()
        self.assertEqual(job.retries_left, 1)
        job.complete()
        job = self.client.jobs[jid]
        self.assertEqual(job.retries_left, 2)

    def test_retries_put(self):
        '''When moved to a new queue, jobs get their original retries'''
        jid = self.queue.put('Foo', 0, {'test': 'retries_put'}, retries=2)
        self.client.config['heartbeat'] = -10
        job = self.queue.pop()
        self.assertNotEqual(job, None)
        job = self.queue.pop()
        self.assertEqual(job.retries_left, 1)
        job.move('testing')
        job = self.client.jobs[jid]
        self.assertEqual(job.retries_left, 2)

    def test_stats_failed(self):
        '''Stats accurately report the number of failed items'''
        jid = self.queue.put('Foo', 0, {'test': 'stats_failed'})
        stats = self.queue.stats()
        self.assertEqual(stats['failed'], 0)
        self.assertEqual(stats['failures'], 0)
        job = self.queue.pop()
        job.fail('foo', 'bar')
        stats = self.queue.stats()
        self.assertEqual(stats['failed'], 1)
        self.assertEqual(stats['failures'], 1)
        job.move('testing')
        stats = self.queue.stats()
        self.assertEqual(stats['failed'], 0)
        self.assertEqual(stats['failures'], 1)

    def test_stats_retries(self):
        '''Stats accurately knows how many retries have happened'''
        self.queue.put('Foo', 0, {'test': 'stats_retries'})
        self.client.config['heartbeat'] = -10
        self.queue.pop()
        self.assertEqual(self.queue.stats()['retries'], 0)
        self.queue.pop()
        self.assertEqual(self.queue.stats()['retries'], 1)

    def test_workers(self):
        '''We should know how many jobs different workers have'''
        jid = self.queue.put('Foo', 0, {'test': 'workers'})
        self.assertEqual(self.client.workers.counts, {})
        self.queue.pop()
        self.assertEqual(self.client.workers.counts, [{
            'name': self.queue.worker_name,
            'jobs': 1,
            'stalled': 0
        }])
        # Now get specific worker information
        self.assertEqual(self.client.workers[self.queue.worker_name], {
            'jobs': [jid],
            'stalled': []
        })

    def test_workers_cancel(self):
        '''When a job is canceled, it disappears from that worker's jobs'''
        jid = self.queue.put('Foo', 0, {'test': 'workers_cancel'})
        job = self.queue.pop()
        self.assertEqual(self.client.workers.counts, [{
            'name': self.queue.worker_name,
            'jobs': 1,
            'stalled': 0
        }])
        self.assertEqual(self.client.workers[self.queue.worker_name], {
            'jobs': [jid],
            'stalled': []
        })
        # Now cancel the job
        job.cancel()
        self.assertEqual(self.client.workers.counts, [{
            'name': self.queue.worker_name,
            'jobs': 0,
            'stalled': 0
        }])
        self.assertEqual(self.client.workers[self.queue.worker_name], {
            'jobs': [],
            'stalled': []
        })

    def test_workers_lost_lock(self):
        '''When a worker loses a lock, it shouldn't show up in its jobs'''
        jid = self.queue.put('Foo', 0, {'test': 'workers_lost_lock'})
        self.client.config['heartbeat'] = -10
        self.queue.pop()
        self.assertEqual(self.client.workers.counts, [{
            'name': self.queue.worker_name,
            'jobs': 0,
            'stalled': 1
        }])
        self.assertEqual(self.client.workers[self.queue.worker_name], {
            'jobs': [],
            'stalled': [jid]
        })
        # Now, let's pop it with a different worker
        del self.client.config['heartbeat']
        self.worker_a.pop()
        self.assertEqual(self.client.workers.counts, [{
            'name': self.worker_a.worker_name,
            'jobs': 1,
            'stalled': 0
        }, {
            'name': self.queue.worker_name,
            'jobs': 0,
            'stalled': 0
        }])
        self.assertEqual(self.client.workers[self.queue.worker_name], {
            'jobs': [],
            'stalled': []
        })

    def test_workers_fail(self):
        '''When a worker failed a job, shouldn't appear in its jobs'''
        jid = self.queue.put('Foo', 0, {'test': 'workers_fail'})
        job = self.queue.pop()
        self.assertEqual(self.client.workers.counts, [{
            'name': self.queue.worker_name,
            'jobs': 1,
            'stalled': 0
        }])
        self.assertEqual(self.client.workers[self.queue.worker_name], {
            'jobs': [jid],
            'stalled': []
        })
        # Now, let's fail it
        job.fail('foo', 'bar')
        self.assertEqual(self.client.workers.counts, [{
            'name': self.queue.worker_name,
            'jobs': 0,
            'stalled': 0
        }])
        self.assertEqual(self.client.workers[self.queue.worker_name], {
            'jobs': [],
            'stalled': []
        })

    def test_workers_complete(self):
        '''When a worker completes a job, it shows up in workers correctly'''
        jid = self.queue.put('Foo', 0, {'test': 'workers_complete'})
        job = self.queue.pop()
        self.assertEqual(self.client.workers.counts, [{
            'name': self.queue.worker_name,
            'jobs': 1,
            'stalled': 0
        }])
        self.assertEqual(self.client.workers[self.queue.worker_name], {
            'jobs': [jid],
            'stalled': []
        })
        # Now complete it
        job.complete()
        self.assertEqual(self.client.workers.counts, [{
            'name': self.queue.worker_name,
            'jobs': 0,
            'stalled': 0
        }])
        self.assertEqual(self.client.workers[self.queue.worker_name], {
            'jobs': [],
            'stalled': []
        })

    def test_workers_reput(self):
        '''When a job is put onto another queue...'''
        jid = self.queue.put('Foo', 0, {'test': 'workers_reput'})
        job = self.queue.pop()
        self.assertEqual(self.client.workers.counts, [{
            'name': self.queue.worker_name,
            'jobs': 1,
            'stalled': 0
        }])
        self.assertEqual(self.client.workers[self.queue.worker_name], {
            'jobs': [jid],
            'stalled': []
        })
        job.move('other')
        self.assertEqual(self.client.workers.counts, [{
            'name': self.queue.worker_name,
            'jobs': 0,
            'stalled': 0
        }])
        self.assertEqual(self.client.workers[self.queue.worker_name], {
            'jobs': [],
            'stalled': []
        })


if __name__ == '__main__':
    import unittest
    unittest.main()
