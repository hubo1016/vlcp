'''
Created on 2017/9/29

:author: hubo
'''
import unittest
from random import randrange, sample
from vlcp.utils.indexedheap import IndexedHeap

class Test(unittest.TestCase):


    def testRandomSort(self):
        data = [(randrange(0,10000), randrange(0,10000)) for _ in range(0,1000)]
        data = list((v,k)
                    for k, v in dict((d[1], d[0]) for d in data).items())
        heap = IndexedHeap()
        for d in data:
            heap.push(d[1], d)
        # Remove min item
        minv = heap.top()
        self.assertEqual(minv, min(data)[1])
        heap.remove(minv)
        data.remove(min(data))
        # Remove last
        last = heap.heap[-1][0]
        heap.remove(last[1])
        data.remove(last)
        self.assertEqual(len(heap), len(data))
        # Random remove
        remove_sample = sample(data, 100)
        data = [d for d in data if d not in remove_sample]
        for d in remove_sample:
            heap.remove(d[1])
        result = []
        while heap:
            result.append(heap.pop())
        self.assertListEqual(result, [d[1] for d in sorted(data)])
    
    def testPending(self):
        heap = IndexedHeap()
        heap.push(1, 1)
        self.assertEqual(len(heap), 1)
        self.assertEqual(len(heap.heap), 1)
        heap.push(2, 3)
        self.assertEqual(len(heap), 2)
        self.assertEqual(len(heap.heap), 1)
        self.assertEqual(len(heap.pending), 1)
        heap.push(3, 2)
        self.assertEqual(len(heap), 3)
        self.assertEqual(len(heap.heap), 1)
        self.assertEqual(len(heap.pending), 2)
        self.assertEqual(heap.pendingpriority, 2)
        heap.push(4, 4)
        self.assertEqual(len(heap), 4)
        self.assertEqual(len(heap.heap), 1)
        self.assertEqual(len(heap.pending), 3)
        self.assertEqual(heap.pendingpriority, 2)
        heap.remove(4)
        self.assertEqual(len(heap), 3)
        self.assertEqual(len(heap.heap), 1)
        self.assertEqual(len(heap.pending), 2)
        self.assertEqual(heap.pendingpriority, 2)
        self.assertEqual(heap.pop(), 1)
        self.assertEqual(len(heap), 2)
        self.assertEqual(len(heap.heap), 2)
        self.assertEqual(len(heap.pending), 0)
        self.assertIsNone(heap.pendingpriority)
        heap.push(1, 1)
        self.assertEqual(len(heap), 3)
        self.assertEqual(len(heap.heap), 3)
        self.assertEqual(len(heap.pending), 0)
        self.assertIsNone(heap.pendingpriority)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testRandomSort']
    unittest.main()