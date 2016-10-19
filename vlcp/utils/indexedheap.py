'''
Created on 2016/10/18

:author: hubo
'''


class IndexedHeap(object):
    '''
    A heap with indices
    '''
    def __init__(self):
        self.heap = []
        self.index = {}
    def push(self, value, priority):
        if value in self.index:
            self.setpriority(value, priority)
        else:
            self.heap.append((priority, value))
            self.index[value] = len(self.heap) - 1
            self._siftup(len(self.heap) - 1)
    def remove(self, value):
        if value in self.index:
            pos = self.index[value]
            del self.index[value]
            if pos == len(self.heap) - 1:
                del self.heap[-1]
                return
            ci = self.heap[pos]
            li = self.heap.pop()
            self.heap[pos] = li
            self.index[li[1]] = pos
            if li[0] > ci[0]:
                self._siftdown(pos)
            else:
                self._siftup(pos)
    def pop(self):
        if not self.heap:
            raise IndexError('pop from an empty heap')
        ret = self.heap[0]
        del self.index[ret[1]]
        li = self.heap.pop()
        if not self.heap:
            return ret[1]
        self.heap[0] = li
        self.index[li[1]] = 0
        self._siftdown(0)
        return ret[1]
    def poppush(self, value, priority):
        if not self.heap:
            raise IndexError('pop from an empty heap')
        ret = self.heap[0]
        del self.index[ret[1]]
        self.heap[0] = (priority, value)
        self.index[value] = priority
        self._siftdown(0)
        return ret[1]
    def pushpop(self, value, priority):
        if not self.heap or priority < self.heap[0][0]:
            return value
        return self.poppush(value, priority)
    def setpriority(self, value, priority):
        pos = self.index[value]
        temp = self.heap[pos]
        self.heap[pos] = (priority, value)
        if temp[0] < priority:
            self._siftdown(pos)
        else:
            self._siftup(pos)
    def replace(self, value, value2):
        pos = self.index[value]
        del self.index[value]
        self.heap[pos] = (self.heap[pos][0], value2)
    def clear(self):
        self.index.clear()
        del self.heap[:]
    def top(self):
        return self.heap[0][1]
    def topPriority(self):
        return self.heap[0][0]
    def _siftup(self, pos):
        temp = self.heap[pos]
        while pos > 0:
            pindex = (pos - 1) // 2
            pt = self.heap[pindex]
            if pt[0] > temp[0]:
                self.heap[pos] = pt
                self.index[pt[1]] = pos
            else:
                break
            pos = pindex
        self.heap[pos] = temp
        self.index[temp[1]] = pos
    def _siftdown(self, pos):
        temp = self.heap[pos]
        l = len(self.heap)
        while pos * 2 + 1 < l:
            cindex = pos * 2 + 1
            pt = self.heap[cindex]
            if cindex + 1 < l and self.heap[cindex+1][0] < pt[0]:
                cindex = cindex + 1
                pt = self.heap[cindex]
            if pt[0] < temp[0]:
                self.heap[pos] = pt
                self.index[pt[1]] = pos
            else:
                break
            pos = cindex
        self.heap[pos] = temp
        self.index[temp[1]] = pos
    def __len__(self):
        return len(self.heap)
    def __nonzero__(self):
        return bool(self.heap)
    def __contains__(self, value):
        return value in self.index
