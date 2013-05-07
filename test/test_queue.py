#! /usr/bin/env python

'''Test some properties about the queues'''

from common import TestQsome


class TestQueue(TestQsome):
    '''Some basic sanity checks'''
    def test_subqueues(self):
        '''Make sure that we can always list all of the subqueues of a given
        queue'''
        queue = self.client.queues['testing']
        # As a first test, if a queue doesn't exist, it shouldn't have any
        # subqueues.
        self.assertEqual(queue.subqueues(), ['testing-1'])

        # Next, we want to make sure that we can grow this queue and see that
        # reflected in the subqueues
        for size in [10, 20, 30, 50, 1, 20, 30, 50]:
            queue.resize(size)
            self.assertEqual(len(queue.subqueues()), size)

    def test_config(self):
        '''Make sure the queue configuration operations work'''
        queue = self.client.queues['testing']
        queue.config('foo', 'bar')
        queue.config('bar', 'whiz')
        self.assertEqual(queue.config(), {
            'foo': 'bar', 'bar': 'whiz'
        })

        # Let's try set / get
        self.assertEqual(queue.config('foo'), 'bar')
        queue.config('foo', 'whiz')
        self.assertEqual(queue.config('foo'), 'whiz')

    def test_concurrency_limit(self):
        '''Make sure we can limit the concurrency of a queue'''
        queue = self.client.queues['testing']
        queue.resize(10)
        for count in range(100):
            queue.put('Foo', count, {'test': 'test_concurrency_limit'})

        # Ensure we can only pop off 10 jobs
        jobs = queue.pop(100)
        self.assertEqual(len(jobs), 10)

        # Now we'll set the concurrency of each subqueue in the queue
        queue.config('concurrency', 2)
        jobs.extend(queue.pop(100))
        self.assertEqual(len(jobs), 20)

    def test_mutex(self):
        '''Ensure that we can only ever have a single job from each of the
        subqueues in a queue'''
        # We'll put a bunch of jobs into the queue, ensuring that there are
        # enough to have multiple jobs in each subqueue
        self.queue.resize(5)
        for count in range(10):
            self.queue.put('Foo', count, {'test': 'test_mutex'})

        # We should only be able to get 5 jobs back, and they should be jobs
        # with hashes 0-4
        jobs = self.queue.pop(10)
        self.assertEqual(len(jobs), 5)
        self.assertEqual([j.hash for j in jobs], range(5))

    def test_complete_pop(self):
        '''Ensure that after completing jobs, new jobs from the same subqueue
        become available'''
        self.queue.resize(10)
        for count in range(50):
            self.queue.put('Foo', count, {'test': 'test_complete_pop'})

        jobs = self.queue.pop(20)
        self.assertEqual(len(jobs), 10)
        for job in jobs:
            job.complete()

        # After completing these jobs, we should be able to get more
        jobs = self.queue.pop(20)
        self.assertEqual(len(jobs), 10)

    def test_grow(self):
        '''Ensure that we can grow a queue'''
        # We'll begin by sizing this queue to 5, adding 20 jobs and ensuring
        # that we get 5 jobs when we pop.
        self.queue.resize(5)
        for count in range(20):
            self.queue.put('Foo', count, {'test': 'test_grow'})

        jobs = self.queue.pop(20)
        self.assertEqual(len(jobs), 5)

        # After resizing it to 10, we should have five more jobs available
        self.queue.resize(10)
        jobs = self.queue.pop(20)
        self.assertEqual(len(jobs), 5)

    def test_shrink(self):
        '''Ensure that we can shrink a queue'''
        # When shrinking a queue, we should see that all the currently active
        # jobs should be allowed to complete correctly.
        self.queue.resize(10)
        for count in range(20):
            self.queue.put('Foo', count, {'test': 'test_shrink'})

        jobs = self.queue.pop(20)
        self.assertEqual(len(jobs), 10)
        first = jobs[0:5]
        second = jobs[5:]

        # Now let's shrink it, complete some jobs, and then ensure that we have
        self.queue.resize(5)
        for job in first:
            job.complete()

        self.assertEqual(len(self.queue.pop(20)), 0)

        # Complete the rest of the jobs, and ensure we can see the rest
        for job in second:
            job.complete()

        self.assertEqual(len(self.queue.pop(20)), 5)


if __name__ == '__main__':
    import unittest
    unittest.main()
