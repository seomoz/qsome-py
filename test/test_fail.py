#! /usr/bin/env python

'''A group of tests about failures'''

from common import TestQsome

import unittest

from qless.exceptions import LostLockException, QlessException


class TestFail(TestQsome):
    def test_fail_failed(self):
        '''After failing a job, we can get a list of failed jobs'''
        self.assertEqual(len(self.client.jobs.failed()), 0)
        jid = self.queue.put('Foo', 0, {'test': 'fail_failed'})
        job = self.queue.pop()
        job.fail('foo', 'Some sort of message')
        self.assertEqual(self.queue.pop(), None)
        self.assertEqual(self.client.jobs.failed(), {
            'foo': 1
        })
        results = self.client.jobs.failed('foo')
        self.assertEqual(results['total'], 1)
        job = results['jobs'][0]
        self.assertEqual(job.jid, jid)
        self.assertEqual(job.queue_name, 'testing-1')
        self.assertEqual(job.data, {'test': 'fail_failed'})
        self.assertEqual(job.worker_name, '')
        self.assertEqual(job.state, 'failed')
        self.assertEqual(job.retries_left, 5)
        self.assertEqual(job.original_retries, 5)
        self.assertEqual(job.klass_name, 'Foo')
        self.assertEqual(job.tags, [])
        self.assertEqual(job.hash, 0)

    def test_pop_fail(self):
        '''Only popped jobs can fail, and failed can't be completed'''
        self.assertEqual(len(self.client.jobs.failed()), 0)
        jid = self.queue.put('Foo', 0, {'test': 'pop_fail'})
        job = self.queue.pop()
        self.assertNotEqual(job, None)
        job.fail('foo', 'Some sort of message')
        self.assertEqual(len(self.queue), 0)
        self.assertRaises(LostLockException, job.heartbeat)
        self.assertRaises(QlessException, job.complete)
        self.assertEqual(self.client.jobs.failed(), {
            'foo': 1
        })
        results = self.client.jobs.failed('foo')
        self.assertEqual(results['total'], 1)
        self.assertEqual(results['jobs'][0].jid, jid)

    def test_fail_state(self):
        '''We can only fail running jobs'''
        self.assertEqual(len(self.client.jobs.failed()), 0)
        job = self.client.jobs[
            self.queue.put('Foo', 0, {'test': 'fail_state'})]
        self.assertRaises(
            QlessException, job.fail, 'foo', 'Some sort of message')
        self.assertEqual(len(self.client.jobs.failed()), 0)
        job = self.client.jobs[
            self.queue.put('Foo', 0, {'test': 'fail_state'}, delay=60)]
        self.assertRaises(
            QlessException, job.fail, 'foo', 'Some sort of message')
        self.assertEqual(len(self.client.jobs.failed()), 0)
        self.queue.put('Foo', 0, {'test': 'fail_complete'})
        job = self.queue.pop()
        job.complete()
        self.assertRaises(
            QlessException, job.fail, 'foo', 'Some sort of message')
        self.assertEqual(len(self.client.jobs.failed()), 0)

    def test_put_failed(self):
        '''Moving a failed job into another queue'''
        self.queue.put('Foo', 0, {'test': 'put_failed'})
        job = self.queue.pop()
        job.fail('foo', 'some message')
        self.assertEqual(self.client.jobs.failed(), {'foo': 1})
        job.move('testing')
        self.assertEqual(len(self.queue), 1)
        self.assertEqual(self.client.jobs.failed(), {})

    def test_complete_failed(self):
        '''No failure information after a job completes'''
        jid = self.queue.put('Foo', 0, {'test': 'put_failed'})
        job = self.queue.pop()
        job.fail('foo', 'some message')
        job.move('testing')
        job = self.queue.pop()
        self.assertEqual(job.complete(), 'complete')
        self.assertEqual(self.client.jobs[jid].failure, {})

    # def test_unfail(self):
    #     '''Unfail large number of jobs at once'''
    #     self.queue.resize(1000)
    #     jids = [self.queue.put('Foo', i, {'test': 'test_unfail'})
    #         for i in range(1000)]
    #     self.assertEqual(len(
    #         [job.fail('foo', 'bar') for job in self.queue.pop(1000)]), 1000)
    #     self.assertEqual(self.client.unfail('foo', self.queue.name, 25), 25)
    #     self.assertEqual(self.client.jobs.failed()['foo'], 975)
    #     failed = self.client.jobs.failed('foo', 0, 1000)['jobs']
    #     self.assertEqual(set([j.jid for j in failed]), set(jids[25:]))

    #     # Now make sure that they can be popped off and completed correctly
    #     self.assertEqual(
    #         [j.complete() for j in self.queue.pop(1000)],
    #         ['complete'] * 25)

if __name__ == '__main__':
    unittest.main()
