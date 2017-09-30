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
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testRandomSort']
    unittest.main()