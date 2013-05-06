#! /usr/bin/env python

'''Test some properties about the queues'''

from common import TestQsome


class TestBasic(TestQsome):
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


if __name__ == '__main__':
    import unittest
    unittest.main()
