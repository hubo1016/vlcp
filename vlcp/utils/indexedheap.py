'''
Created on 2016/10/18

:author: hubo
'''

class IndexedHeap(object):
    '''
    A heap with indices
    '''
    __slots__ = ('heap', 'index', 'pending', 'pendingpriority')
    def __init__(self):
        self.heap = []
        self.index = {}
        self.pending = {}
        self.pendingpriority = None
    def push(self, value, priority):
        if value in self.index:
            self.setpriority(value, priority)
        else:
            if self.heap and self.heap[0][0] < priority:
                # pending
                self.index[value] = None
                self.pending[value] = priority
                if self.pendingpriority is None or priority < self.pendingpriority:
                    self.pendingpriority = priority
            else:
                self._push(value, priority)
    def _push(self, value, priority):
        self.heap.append((priority, value))
        self.index[value] = len(self.heap) - 1
        self._siftup(len(self.heap) - 1)
    
    def _check_pending(self):
        if self.pending and (not self.heap or self.pendingpriority <= self.heap[0][0]):
            for k,v in self.pending.items():
                self._push(k, v)
            self.pending.clear()
            self.pendingpriority = None
    
    def remove(self, value):
        if value in self.index:
            pos = self.index.pop(value)
            if pos is None:
                del self.pending[value]
                if not self.pending:
                    self.pendingpriority = None
                return
            if pos == len(self.heap) - 1:
                del self.heap[-1]
                self._check_pending()
                return
            ci = self.heap[pos]
            li = self.heap.pop()
            self.heap[pos] = li
            self.index[li[1]] = pos
            if li[0] > ci[0]:
                self._siftdown(pos)
            else:
                self._siftup(pos)
            if pos == 0:
                self._check_pending()
    def pop(self):
        if not self.heap:
            raise IndexError('pop from an empty heap')
        ret = self.heap[0]
        del self.index[ret[1]]
        li = self.heap.pop()
        if not self.heap:
            self._check_pending()
            return ret[1]
        self.heap[0] = li
        self.index[li[1]] = 0
        self._siftdown(0)
        self._check_pending()
        return ret[1]
    def poppush(self, value, priority):
        if not self.heap:
            raise IndexError('pop from an empty heap')
        ret = self.heap[0]
        del self.index[ret[1]]
        self.heap[0] = (priority, value)
        self.index[value] = 0
        self._siftdown(0)
        self._check_pending()
        return ret[1]
    def pushpop(self, value, priority):
        if not self.heap or priority < self.heap[0][0]:
            return value
        return self.poppush(value, priority)
    def setpriority(self, value, priority):
        pos = self.index[value]
        if pos is None:
            if priority <= self.heap[0][0]:
                del self.pending[value]
                self._push(value, priority)
            else:
                self.pending[value] = priority
                if priority < self.pendingpriority:
                    self.pendingpriority = priority
            return
        temp = self.heap[pos]
        self.heap[pos] = (priority, value)
        if temp[0] < priority:
            self._siftdown(pos)
        else:
            self._siftup(pos)
        self._check_pending()
    def replace(self, value, value2):
        pos = self.index[value]
        del self.index[value]
        if pos is None:
            self.pending[value2] = self.pending[value]
        else:
            self.heap[pos] = (self.heap[pos][0], value2)
            self.index[value2] = pos
    def clear(self):
        self.index.clear()
        del self.heap[:]
        self.pending.clear()
        self.pendingpriority = None
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
        return len(self.index)
    def __nonzero__(self):
        return bool(self.index)
    def __contains__(self, value):
        return value in self.index
