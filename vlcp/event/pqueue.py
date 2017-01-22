'''
Created on 2015/06/02

:author: hubo
'''
from __future__ import print_function, absolute_import, division 

try:
    from vlcp_event_cython.pqueue import *
except:

    from collections import deque
    from .matchtree import MatchTree
    from .event import Event, withIndices
    from bisect import bisect_left
    from heapq import heappush, heappop

    @withIndices('queue')
    class QueueCanWriteEvent(Event):
        pass

    @withIndices('queue')
    class QueueIsEmptyEvent(Event):
        pass

    @withIndices('newonly', 'firstonly')
    class AutoClassQueueCanWriteEvent(QueueCanWriteEvent):
        pass

    class CBQueue(object):
        '''
        A multi-queue model with priority and balance.
        When first created, there is a default queue with priority 0. More sub-queues maybe created with addSubQueue.
        Each sub-queue is a CBQueue which accepts more sub-queues. Sub-queues are considered as black-box to the outer parent.
        '''
        class FifoQueue(object):
            '''
            A wrapper for a FIFO queue
            '''
            def __init__(self, parent = None, maxlength = None):
                self.queue = deque()
                self.parent = parent
                self.maxlength = maxlength
                self.blocked = False
                if self.maxlength is not None and self.maxlength <= 0:
                    self.maxlength = 1
                self.isWaited = False
            def append(self, value, force = False):
                if not force and not self.canAppend():
                    self.isWaited = True
                    return QueueCanWriteEvent.createMatcher(self)
                if self.parent is not None:
                    m = self.parent.notifyAppend(self, force)
                    if m is not None:
                        return m
                self.queue.append(value)
                return None
            def canAppend(self):
                return self.maxlength is None or len(self.queue) < self.maxlength
            def canPop(self):
                return self.queue and not self.blocked
            def pop(self):
                ret = self._pop()
                if self.parent is not None:
                    pr = self.parent.notifyPop(self)
                    ret[1].extend(pr[0])
                    ret[2].extend(pr[1])
                return ret
            def _pop(self):
                if self.blocked:
                    raise IndexError('pop from a blocked queue')
                ret = self.queue.popleft()
                if self.isWaited and self.canAppend():
                    self.isWaited = False
                    return (ret, [QueueCanWriteEvent(self)], [])
                else:
                    return (ret, [], [])
            def clear(self):
                l = len(self)
                ret = self._clear()
                if self.parent is not None:
                    pr = self.parent.notifyPop(self, l)
                    ret[0].extend(pr[0])
                    ret[1].extend(pr[1])
                return ret
            def _clear(self):
                if self.blocked:
                    self.unblockall()
                self.queue.clear()
                if self.isWaited and self.canAppend():
                    self.isWaited = False
                    return ([QueueCanWriteEvent(self)], [])
                else:
                    return ([], [])
            def __len__(self):
                return len(self.queue)
            def block(self, value):
                if self.parent is not None:
                    self.parent.notifyAppend(self, True)
                self.queue.appendleft(value)
                if not self.blocked:
                    self.blocked = True
                    if self.parent is not None:
                        self.parent.notifyBlock(self, True)
            def unblock(self, value):
                if self.blocked:
                    self.blocked = False
                    if self.parent is not None:
                        self.parent.notifyBlock(self, False)
            def unblockall(self):
                if self.blocked:
                    self.blocked = False
                    if self.parent is not None:
                        self.parent.notifyBlock(self, False)
        class PriorityQueue(object):
            '''
            A queue with inner built priority. Event must have a "priority" property to use with this type of queue.
            For fail-safe, events without "priority" property have the lowest priority.
            
            NOTICE: different from the queue priority, the priority property is smaller-higher, and is not limited to integers.
            This allows datetime to be used as an increasing priority
            '''
            def __init__(self, parent = None, maxlength = None, key = 'priority'):
                # a heap
                self.queue = []
                self.deque = deque()
                self.parent = parent
                self.maxlength = maxlength
                self.blocks = set()
                if self.maxlength is not None and self.maxlength <= 0:
                    self.maxlength = 1
                self.blocked = False
                self.isWaited = False
                self.key = key
            @classmethod
            def initHelper(cls, key = 'priority'):
                def initer(parent = None, maxlength = None):
                    return cls(parent, maxlength, key)
                return initer
            def append(self, value, force = False):
                if not force and not self.canAppend():
                    self.isWaited = True
                    return QueueCanWriteEvent.createMatcher(self)
                if self.parent is not None:
                    m = self.parent.notifyAppend(self, force)
                    if m is not None:
                        return m
                if hasattr(value, self.key):
                    heappush(self.queue, (getattr(value, self.key), value))
                    # a priority push may change the block status
                    if self.blocked and not self.queue[0][1] in self.blocks:
                        self.blocked = False
                        if self.parent is not None:
                            self.parent.notifyBlock(self, False)
                else:
                    self.deque.append(value)
                return None
            def canAppend(self):
                return self.maxlength is None or len(self.queue) + len(self.deque) < self.maxlength
            def canPop(self):
                return len(self.queue) + len(self.deque) > 0 and not self.blocked
            def pop(self):
                ret = self._pop()
                if self.parent is not None:
                    pr = self.parent.notifyPop(self)
                    ret[1].extend(pr[0])
                    ret[2].extend(pr[1])
                return ret
            def _pop(self):
                if self.blocked:
                    raise IndexError('pop from a blocked queue')
                if self.queue:
                    ret = heappop(self.queue)[1]
                else:
                    ret = self.deque.popleft()
                if self.queue:
                    blocked = self.queue[0][1] in self.blocks
                elif self.deque:
                    blocked = self.deque[0] in self.blocks
                else:
                    blocked = False
                if self.blocked != blocked:
                    self.blocked = blocked
                    if self.parent is not None:
                        self.parent.notifyBlock(self, blocked)
                if self.isWaited and self.canAppend():
                    self.isWaited = False
                    return (ret, [QueueCanWriteEvent(self)], [])
                else:
                    return (ret, [], [])
            def _clear(self):
                if self.blocks:
                    self.unblockall()
                del self.queue[:]
                self.deque.clear()
                if self.isWaited and self.canAppend():
                    self.isWaited = False
                    return ([QueueCanWriteEvent(self)], [])
                else:
                    return ([], [])
            def clear(self):
                l = len(self)
                ret = self._clear()
                if self.parent is not None:
                    pr = self.parent.notifyPop(self, l)
                    ret[0].extend(pr[0])
                    ret[1].extend(pr[1])
                return ret
            def __len__(self):
                return len(self.queue) + len(self.deque)
            def block(self, value):
                self.blocks.add(value)
                if self.parent is not None:
                    self.parent.notifyAppend(self, True)
                if hasattr(value, self.key):
                    heappush(self.queue, (getattr(value, self.key), value))
                else:
                    self.deque.appendleft(value)
                if self.queue:
                    blocked = self.queue[0][1] in self.blocks
                elif self.deque:
                    blocked = self.deque[0] in self.blocks
                else:
                    blocked = False
                if self.blocked != blocked:
                    self.blocked = blocked
                    if self.parent is not None:
                        self.parent.notifyBlock(self, blocked)
            def unblock(self, value):
                self.blocks.remove(value)
                if self.queue:
                    blocked = self.queue[0][1] in self.blocks
                elif self.deque:
                    blocked = self.deque[0] in self.blocks
                else:
                    blocked = False
                if self.blocked != blocked:
                    self.blocked = blocked
                    if self.parent is not None:
                        self.parent.notifyBlock(self, blocked)
            def unblockall(self):
                self.blocks.clear()
                blocked = False
                if self.blocked != blocked:
                    self.blocked = blocked
                    if self.parent is not None:
                        self.parent.notifyBlock(self, blocked)
        class MultiQueue(object):
            '''
            A multi-queue container, every queue in a multi-queue has the same priority, and is popped in turn.
            '''
            class CircleListNode(object):
                '''
                Circle link list
                '''
                def __init__(self, value):
                    self.prev = self
                    self.value = value
                    self.next = self
                def insertprev(self, node):
                    self.prev.next = node
                    node.prev = self.prev
                    node.next = self
                    self.prev = node
                    return self
                def remove(self):
                    if self.next is self:
                        return None
                    self.prev.next = self.next
                    self.next.prev = self.prev
                    ret = self.next
                    self.next = self
                    self.prev = self
                    return ret
            class CircleList(object):
                def __init__(self):
                    self.current = None
                def remove(self, node):
                    if self.current is node:
                        self.current = node.remove()
                    else:
                        node.remove()
                def insertprev(self, node):
                    if self.current is None:
                        self.current = node
                    else:
                        self.current.insertprev(node)
                def insertcurrent(self, node):
                    self.insertprev(node)
                    self.current = node
                def next(self):
                    ret = self.current
                    if self.current is not None:
                        self.current = self.current.next
                    return ret
                def clear(self):
                    self.current = None
            def __init__(self, parent = None, priority = 0):
                self.queues = CBQueue.MultiQueue.CircleList()
                self.queueDict = {}
                self.queueStat = {}
                self.statseq = deque()
                self.parent = parent
                self.priority = priority
                self.totalSize = 0
                self.blocked = True
            def canPop(self):
                return bool(self.queues.current)
            def _pop(self):
                if not self.canPop():
                    raise IndexError('pop from an empty or blocked queue')
                c = self.queues.next()
                ret = c.value._pop()
                self.queueStat[c.value] = self.queueStat.get(c.value, 0) + 1
                while len(self.statseq) >= 10 * len(self.queueDict) + 10:
                    o = self.statseq.popleft()
                    if o in self.queueStat:
                        self.queueStat[o] = self.queueStat[o] - 1
                        if self.queueStat[o] <= 0 and not o in self.queueDict:
                            del self.queueStat[o]
                self.statseq.append(c.value)
                if not c.value.canPop():
                    self.queues.remove(c)
                    self.queueDict[c.value] = None
                self.totalSize = self.totalSize - 1
                if not self.canPop():
                    if not self.blocked:
                        self.blocked = True
                        if self.parent is not None:
                            self.parent.notifyBlock(self, True)
                return ret            
            def addSubQueue(self, queue):
                self.totalSize = self.totalSize + len(queue)
                queue.parent = self
                if queue.canPop():
                    # Activate this queue
                    node = CBQueue.MultiQueue.CircleListNode(queue)
                    self.queues.insertprev(node)
                    self.queueDict[queue] = node
                    self.queueStat[queue] = 0
                else:
                    self.queueDict[queue] = None
                if self.canPop():
                    if self.blocked:
                        self.blocked = False
                        if self.parent is not None:
                            self.parent.notifyBlock(self, False)
            def removeSubQueue(self, queue):
                self.totalSize = self.totalSize - len(queue)
                if self.queueDict[queue] is not None:
                    self.queues.remove(self.queueDict[queue])
                del self.queueDict[queue]
                if queue in self.queueStat:
                    del self.queueStat[queue]
                if not self.canPop():
                    if not self.blocked:
                        self.blocked = True
                        if self.parent is not None:
                            self.parent.notifyBlock(self, True)
            def notifyAppend(self, queue, force):
                if self.parent is not None:
                    m = self.parent.notifyAppend(self, force)
                    if m is not None:
                        return m
                self.totalSize = self.totalSize + 1
                if not queue.blocked:
                    if self.queueDict[queue] is None:
                        # Activate this queue
                        node = CBQueue.MultiQueue.CircleListNode(queue)
                        qs = self.queueStat.setdefault(queue, 0)
                        if qs * len(self.queueStat) >= len(self.statseq):
                            self.queues.insertprev(node)
                        else:
                            self.queues.insertcurrent(node)
                        self.queueDict[queue] = node
                if self.canPop():
                    if self.blocked:
                        self.blocked = False
                        if self.parent is not None:
                            self.parent.notifyBlock(self, False)
                return None
            def __len__(self):
                return self.totalSize
            def notifyBlock(self, queue, blocked):
                if queue.canPop():
                    if self.queueDict[queue] is None:
                        # Activate this queue
                        node = CBQueue.MultiQueue.CircleListNode(queue)
                        qs = self.queueStat.setdefault(queue, 0)
                        if qs * len(self.queueStat) >= len(self.statseq):
                            self.queues.insertprev(node)
                        else:
                            self.queues.insertcurrent(node)
                        self.queueDict[queue] = node
                else:
                    if self.queueDict[queue] is not None:
                        self.queues.remove(self.queueDict[queue])
                        self.queueDict[queue] = None
                selfblocked = not self.canPop()
                if selfblocked != self.blocked:
                    self.blocked = selfblocked
                    if self.parent is not None:
                        self.parent.notifyBlock(self, selfblocked)
            def notifyPop(self, queue, length = 1):
                self.totalSize = self.totalSize - length
                if not queue.canPop():
                    if self.queueDict[queue] is not None:
                        self.queues.remove(self.queuDict[queue])
                        self.queueDict[queue] = None
                ret = ([], [])
                if self.parent is not None:
                    ret = self.parent.notifyPop(self, length)
                blocked = not self.canPop()
                if blocked != self.blocked:
                    self.blocked = blocked
                    if self.parent is not None:
                        self.parent.notifyBlock(self, blocked)
                return ret
                
            def unblockall(self):
                for q in self.queueDict.keys():
                    q.unblockall()
            def _clear(self):
                ret = ([],[])
                for q in self.queueDict.keys():
                    pr = q._clear()
                    ret[0].extend(pr[0])
                    ret[1].extend(pr[1])
                self.totalSize = 0
                self.blockedSize = 0
                self.queues.clear()
                if not self.blocked:
                    self.blocked = True
                    if self.parent is not None:
                        self.parent.notifyBlock(self, True)
                return ret
        class AutoClassQueue(object):
            '''
            A queue classify events into virtual sub-queues by key
            '''
            nokey = object()
            def __init__(self, parent = None, maxlength = None, key = 'owner', preserveForNew = 1, maxstat = None, subqueuelimit = None):
                self.queues = CBQueue.MultiQueue.CircleList()
                self.queueDict = {}
                self.queueStat = {}
                self.statseq = deque()
                self.maxlength = maxlength
                self.blocked = False
                if self.maxlength is not None and self.maxlength <= 0:
                    self.maxlength = 1
                if maxstat is None:
                    if self.maxlength is None:
                        self.maxstat = 10240
                    else:
                        self.maxstat = maxlength * 10
                else:
                    self.maxstat = maxstat
                if self.maxstat >= 10240:
                    self.maxstat = 10240
                self.waited = set()
                self.key = key
                self.preserve = preserveForNew
                self.totalSize = 0
                self.blockKeys = set()
                self.subqueuelimit = subqueuelimit
            @classmethod
            def initHelper(cls, key = 'owner', preserveForNew = 1, maxstat = None, subqueuelimit = None):
                def initer(parent = None, maxlength = None):
                    return cls(parent, maxlength, key, preserveForNew, maxstat, subqueuelimit)
                return initer
            def append(self, value, force = False):
                key = getattr(value, self.key, self.nokey)
                # We use hash instead of reference or weakref, this may cause problem, but better thank leak.
                kid = hash(key)
                if not force:
                    w = self._tryAppend(key)
                    if w is not None:
                        return w
                if self.parent is not None:
                    m = self.parent.notifyAppend(self, force)
                    if m is not None:
                        return m
                if key in self.queueDict:
                    self.queueDict[key].value[1].append(value)
                else:
                    node = CBQueue.MultiQueue.CircleListNode((key,deque()))
                    node.value[1].append(value)
                    qs = self.queueStat.setdefault(kid, 0)
                    if qs * len(self.queueStat) >= len(self.statseq):
                        self.queues.insertprev(node)
                    else:
                        self.queues.insertcurrent(node)
                    self.queueDict[key] = node
                self.totalSize += 1
                blocked = not self.canPop() and self.totalSize > 0
                if blocked != self.blocked:
                    self.blocked = blocked
                    if self.parent is not None:
                        self.parent.notifyBlock(self, blocked)
                return None
            def _tryAppend(self, key):
                if self.maxlength is None:
                    if self.subqueuelimit is None or not key in self.queueDict:
                        return None
                    elif len(self.queueDict[key].value[1]) >= self.subqueuelimit:
                        self.waited.add((False, False, key))
                        return AutoClassQueueCanWriteEvent.createMatcher(self, _ismatch = lambda x: x.key == key or x.key is self.nokey)
                    else:
                        return None
                if key in self.queueDict:
                    if self.subqueuelimit is not None and len(self.queueDict[key].value[1]) >= self.subqueuelimit:
                        self.waited.add((False, False, key))
                        return AutoClassQueueCanWriteEvent.createMatcher(self, _ismatch = lambda x: x.key == key or x.key is self.nokey)
                    elif self.totalSize < self.maxlength - self.preserve - len(self.queueStat) + len(self.queueDict):
                        return None
                    else:
                        if len(self.queueDict[key].value[1]) <= 1:
                            self.waited.add((False, True, key))
                            return AutoClassQueueCanWriteEvent.createMatcher(self, False, _ismatch = lambda x: not (x.firstonly and x.key != key))
                        else:
                            self.waited.add((False, False))
                            return AutoClassQueueCanWriteEvent.createMatcher(self, False, False)
                elif hash(key) in self.queueStat:
                    if self.totalSize < self.maxlength - self.preserve:
                        return None
                    else:
                        self.waited.add((False, True))
                        return AutoClassQueueCanWriteEvent.createMatcher(self, False)
                else:
                    if self.totalSize < self.maxlength:
                        return None
                    else:
                        self.waited.add((True, True))
                        return AutoClassQueueCanWriteEvent.createMatcher(self)
            def canAppend(self):
                return self.maxlength is None or self.totalSize < self.maxlength
            def canPop(self):
                return self.queues.current is not None
            def pop(self):
                ret = self._pop()
                if self.parent is not None:
                    pr = self.parent.notifyPop(self)
                    ret[1].extend(pr[0])
                    ret[2].extend(pr[1])
                return ret
            def _pop(self):
                if not self.canPop():
                    raise IndexError('pop from a blocked or empty queue')
                c = self.queues.next()
                key = c.value[0]
                kid = hash(key)
                ret = c.value[1].popleft()
                self.totalSize -= 1
                self.queueStat[kid] = self.queueStat.get(kid, 0) + 1
                while len(self.statseq) >= min(self.maxstat, 10 * len(self.queueStat) + 10):
                    k1 = self.statseq.popleft()
                    self.queueStat[k1] = self.queueStat[k1] - 1
                    if self.queueStat[k1] <= 0:
                        del self.queueStat[k1]
                self.statseq.append(kid)
                if not c.value[1]:
                    del self.queueDict[c.value[0]]
                    self.queues.remove(c)
                blocked = not self.canPop() and self.totalSize > 0
                if blocked != self.blocked:
                    self.blocked = blocked
                    if self.parent is not None:
                        self.parent.notifyBlock(self, blocked)
                if self.waited:
                    if key not in self.queueDict:
                        subsize = 0
                    else:
                        subsize = len(self.queueDict[key].value[1])
                    if self.maxlength is None:
                        if self.subqueuelimit is not None and subsize < self.subqueuelimit and (False, False, key) in self.waited:
                            return (ret, [AutoClassQueueCanWriteEvent(self, False, False, key=key)], [])
                    elif self.totalSize < self.maxlength - self.preserve - len(self.queueStat) + len(self.queueDict):
                        self.waited = set(w for w in self.waited if len(w) == 3 and w[1] == False and w[2] != key)
                        return (ret, [AutoClassQueueCanWriteEvent(self, False, False, key=key)], [])
                    elif self.totalSize < self.maxlength - self.preserve:
                        if (False, True) in self.waited or (False, True, key) in self.waited or (True, True) in self.waited or \
                                (False, False, key) in self.waited:
                            self.waited.discard((False, True))
                            self.waited.discard((False, True, key))
                            self.waited.discard((True, True))
                            self.waited.discard((False, False, key))
                            return (ret, [AutoClassQueueCanWriteEvent(self, False, True, key=key)], [])
                    elif self.totalSize < self.maxlength:
                        if (True, True) in self.waited or (False, False, key) in self.waited or (False, True, key) in self.waited:
                            self.waited.discard((True, True))
                            self.waited.discard((False, False, key))
                            if (False, True, key) in self.waited:
                                self.waited.discard((False, True, key))
                                return (ret, [AutoClassQueueCanWriteEvent(self, False, True, key=key)], [])
                            else:
                                return (ret, [AutoClassQueueCanWriteEvent(self, True, True, key=key)], [])
                    elif self.subqueuelimit is not None and subsize < self.subqueuelimit and (False, False, key) in self.waited:
                        # If we don't wake up the sub-queue waiter now, it may wait forever.
                        # The sub-queue waiter won't be able to send events in, but they will get a new matcher
                        # Some waiters might wake up mistakenly, they will wait again when they try to append the event. 
                        self.waited.discard((True, True))
                        self.waited.discard((False, False, key))
                        return (ret, [AutoClassQueueCanWriteEvent(self, True, True, key=key)], [])
                return (ret, [], [])
            def clear(self):
                l = len(self)
                ret = self._clear()
                if self.parent is not None:
                    pr = self.parent.notifyPop(self, l)
                    ret[0].extend(pr[0])
                    ret[1].extend(pr[1])
                return ret
            def _clear(self):
                self.queues.clear()
                self.blockKeys.clear()
                self.queueDict.clear()
                self.totalSize = 0
                blocked = not self.canPop() and self.totalSize > 0
                if blocked != self.blocked:
                    self.blocked = blocked
                    if self.parent is not None:
                        self.parent.notifyBlock(self, blocked)
                if self.waited:
                    self.waited.clear()
                    return ([AutoClassQueueCanWriteEvent(self, False, False, key=self.nokey)], [])
                else:
                    return ([], [])
            def __len__(self):
                return self.totalSize
            def block(self, value):
                if self.parent is not None:
                    self.parent.notifyAppend(self, True)
                key = getattr(value, self.key, self.nokey)
                if key in self.queueDict:
                    self.queueDict[key].value[1].appendleft(value)
                    self.queues.remove(self.queueDict[key])
                else:
                    node = CBQueue.MultiQueue.CircleListNode((key,deque()))
                    node.value[1].append(value)
                    self.queueDict[key] = node
                self.blockKeys.add(key)
                self.totalSize += 1
                blocked = not self.canPop() and self.totalSize > 0
                if blocked != self.blocked:
                    self.blocked = blocked
                    if self.parent is not None:
                        self.parent.notifyBlock(self, blocked)
            def unblock(self, value):
                key = getattr(value, self.key, self.nokey)
                if key in self.blockKeys:
                    self._unblock(key)
                    blocked = not self.canPop() and self.totalSize > 0
                    if blocked != self.blocked:
                        self.blocked = blocked
                        if self.parent is not None:
                            self.parent.notifyBlock(self, blocked)
            def _unblock(self, key):
                self.blockKeys.remove(key)
                node = self.queueDict[key]
                qs = self.queueStat.setdefault(hash(key), 0)
                if qs * len(self.queueStat) >= len(self.statseq):
                    self.queues.insertprev(node)
                else:
                    self.queues.insertcurrent(node)            
            def unblockall(self):
                for k in list(self.blockKeys):
                    self._unblock(k)
                blocked = not self.canPop() and self.totalSize > 0
                if blocked != self.blocked:
                    self.blocked = blocked
                    if self.parent is not None:
                        self.parent.notifyBlock(self, blocked)
        def __init__(self, tree = None, parent = None, maxdefault = None, maxtotal = None, defaultQueueClass = FifoQueue, defaultQueuePriority = 0):
            '''
            Constructor
            '''
            self.queues = {}
            self.queueindex = {}
            self.prioritySet = []
            if tree is None:
                self.tree = MatchTree()
            else:
                self.tree = tree
            self.parent = parent
            defaultPriority = CBQueue.MultiQueue(self, defaultQueuePriority)
            defaultQueue = defaultQueueClass(defaultPriority, maxdefault)
            defaultPriority.addSubQueue(defaultQueue)
            self.queues[defaultQueuePriority] = defaultPriority
            self.tree.insert(None, defaultQueue)
            self.defaultQueue = defaultQueue
            self.totalSize = 0
            self.maxtotal = maxtotal
            self.blocked = True
            self.blockEvents = {}
            self.isWaited = False
            self.isWaitEmpty = False
            self.outputStat = 0
        def _removeFromTree(self):
            for v in self.queueindex.values():
                if len(v) == 3:
                    v[1]._removeFromTree()
            self.tree.remove(None, self.defaultQueue)
            self.tree = None
        def canAppend(self):
            '''
            Whether the queue is full or not. Only check the total limit. Sub-queue may still be full (even default).
            
            :returns: False if the queue is full, True if not.
                      If there are sub-queues, append() may still fail if the sub-queue is full. 
            '''
            return self.maxtotal is None or self.totalSize < self.maxtotal
        def append(self, event, force = False):
            '''
            Append an event to queue. The events are classified and appended to sub-queues
            
            :param event: input event
            
            :param force: if True, the event is appended even if the queue is full
            
            :returns: None if appended successfully, or a matcher to match a QueueCanWriteEvent otherwise
            '''
            if self.tree is None:
                if self.parent is None:
                    raise IndexError('The queue is removed')
                else:
                    return self.parent.parent.append(event, force)
            q = self.tree.matchfirst(event)
            return q.append(event, force)
        def waitForEmpty(self):
            '''
            Make this queue generate a QueueIsEmptyEvent when it is empty
            
            :returns: matcher for QueueIsEmptyEvent, or None if the queue is already empty
            '''
            if not self:
                return None
            self.isWaitEmpty = True
            return QueueIsEmptyEvent.createMatcher(self)
        def block(self, event, emptyEvents = ()):
            '''
            Return a recently popped event to queue, and block all later events until unblock.
            
            Only the sub-queue directly containing the event is blocked, so events in other queues may still be processed.
            It is illegal to call block and unblock in different queues with a same event.
            
            :param event: the returned event. When the queue is unblocked later, this event will be popped again.
            
            :param emptyEvents: reactivate the QueueIsEmptyEvents
            '''
            q = self.tree.matchfirst(event)
            q.block(event)
            self.blockEvents[event] = q
            for ee in emptyEvents:
                ee.queue.waitForEmpty()
        def unblock(self, event):
            '''
            Remove a block 
            '''
            if event not in self.blockEvents:
                return
            self.blockEvents[event].unblock(event)
            del self.blockEvents[event]
        def unblockqueue(self, queue):
            '''
            Remove blocked events from the queue and all subqueues. Usually used after queue clear/unblockall to prevent leak.
            
            :returns: the cleared events
            '''
            subqueues = set()
            def allSubqueues(q):
                subqueues.add(q)
                subqueues.add(q.defaultQueue)
                for v in q.queueindex.values():
                    if len(v) == 3:
                        allSubqueues(v[1])
            allSubqueues(queue)
            events = [k for k,v in self.blockEvents.items() if v in subqueues]
            for e in events:
                del self.blockEvents[e]
            return events
        def unblockall(self):
            '''
            Remove all blocks from the queue and all sub-queues
            '''
            for q in self.queues.values():
                q.unblockall()
            self.blockEvents.clear()
        def notifyAppend(self, queue, force):
            '''
            Internal notify for sub-queues
            
            :returns: If the append is blocked by parent, an EventMatcher is returned, None else.
            '''
            if not force and not self.canAppend():
                self.isWaited = True
                return QueueCanWriteEvent.createMatcher(self)
            if self.parent is not None:
                m = self.parent.notifyAppend(self, force)
                if m is not None:
                    return m
            self.totalSize = self.totalSize + 1
            return None
        def notifyBlock(self, queue, blocked):
            '''
            Internal notify for sub-queues been blocked
            '''
            if blocked:
                if self.prioritySet[-1] == queue.priority:
                    self.prioritySet.pop()
                else:
                    pindex = bisect_left(self.prioritySet, queue.priority)
                    if pindex < len(self.prioritySet) and self.prioritySet[pindex] == queue.priority:
                        del self.prioritySet[pindex]
            else:
                if queue.canPop():
                    pindex = bisect_left(self.prioritySet, queue.priority)
                    if pindex >= len(self.prioritySet) or self.prioritySet[pindex] != queue.priority:
                        self.prioritySet.insert(pindex, queue.priority)
            newblocked =  not self.canPop()
            if newblocked != self.blocked:
                self.blocked = newblocked
                if self.parent is not None:
                    self.parent.notifyBlock(self, newblocked)
        def notifyPop(self, queue, length = 1):
            '''
            Internal notify for sub-queues been poped
            
            :returns: List of any events generated by this pop
            '''
            self.totalSize = self.totalSize - length
            ret1 = []
            ret2 = []
            if self.isWaited and self.canAppend():
                self.isWaited = False
                ret1.append(QueueCanWriteEvent(self))
            if self.isWaitEmpty and not self:
                self.isWaitEmpty = False
                ret2.append(QueueIsEmptyEvent(self))
            if self.parent is not None:
                pr = self.parent.notifyPop(self, length)
                ret1 += pr[0]
                ret2 += pr[1]
            newblocked =  not self.canPop()
            if newblocked != self.blocked:
                self.blocked = newblocked
                if self.parent is not None:
                    self.parent.notifyBlock(self, newblocked)
            return (ret1, ret2)
        def canPop(self):
            '''
            Whether the queue is empty/blocked or not
            
            :returns: False if the queue is empty or blocked, or True otherwise
            '''
            return bool(self.prioritySet)
        def pop(self):
            '''
            Pop an event from the queue. The event in the queue with higher priority is popped before ones in lower priority.
            If there are multiple queues with the same priority, events are taken in turn from each queue.
            May return some queueEvents indicating that some of the queues can be written into.
            
            :returns: `(obj, (queueEvents,...), (queueEmptyEvents,...))` where obj is the popped event, queueEvents are QueueCanWriteEvents generated by this pop
                      and queueEmptyEvents are QueueIsEmptyEvents generated by this pop
            '''
            ret = self._pop()
            if self.parent is not None:
                pr = self.parent.notifyPop(self)
                ret[1].extend(pr[0])
                ret[2].extend(pr[1])
            return ret
        def _pop(self):
            '''
            Actual pop
            '''
            if not self.canPop():
                raise IndexError('pop from an empty or blocked queue')
            priority = self.prioritySet[-1]
            ret = self.queues[priority]._pop()
            self.outputStat = self.outputStat + 1
            self.totalSize = self.totalSize - 1
            if self.isWaited and self.canAppend():
                self.isWaited = False
                ret[1].append(QueueCanWriteEvent(self))
            if self.isWaitEmpty and not self:
                self.isWaitEmpty = False
                ret[2].append(QueueIsEmptyEvent(self))
            return ret
        def clear(self):
            '''
            Clear all the events in this queue, including any sub-queues.
            
            :returns: ((queueEvents,...), (queueEmptyEvents,...)) where queueEvents are QueueCanWriteEvents generated by clearing.
            '''
            l = len(self)
            ret = self._clear()
            if self.parent is not None:
                pr = self.parent.notifyPop(self, l)
                ret[0].extend(pr[0])
                ret[1].extend(pr[1])
            return ret
        def _clear(self):
            '''
            Actual clear
            '''
            ret = ([],[])
            for q in self.queues.values():
                pr = q._clear()
                ret[0].extend(pr[0])
                ret[1].extend(pr[1])
            self.totalSize = 0
            del self.prioritySet[:]
            if self.isWaited and self.canAppend():
                self.isWaited = False
                ret[0].append(QueueCanWriteEvent(self))
            if self.isWaitEmpty and not self:
                self.isWaitEmpty = False
                ret[1].append(QueueIsEmptyEvent(self))
            self.blockEvents.clear()
            return ret
        def __contains__(self, name):
            return name in self.queueindex
        def __getitem__(self, name):
            '''
            Get a sub-queue through q['sub-queue-name']
            '''
            return self.queueindex[name][1]
        def getPriority(self, queue):
            '''
            get priority of a sub-queue
            '''
            return self.queueindex[queue][0]
        def setPriority(self, queue, priority):
            '''
            Set priority of a sub-queue
            '''
            q = self.queueindex[queue]
            self.queues[q[0]].removeSubQueue(q[1])
            newPriority = self.queues.setdefault(priority, CBQueue.MultiQueue(self, priority))
            q[0] = priority
            newPriority.addSubQueue(q[1])
            
        def addSubQueue(self, priority, matcher, name = None, maxdefault = None, maxtotal = None, defaultQueueClass = FifoQueue):
            '''
            add a sub queue to current queue, with a priority and a matcher
            
            :param priority: priority of this queue. Larger is higher, 0 is lowest.
            
            :param matcher: an event matcher to catch events. Every event match the criteria will be stored in this queue.
            
            :param name: a unique name to identify the sub-queue. If none, the queue is anonymous. It can be any hashable value.
            
            :param maxdefault: max length for default queue.
            
            :param maxtotal: max length for sub-queue total, including sub-queues of sub-queue
            '''
            if name is not None and name in self.queueindex:
                raise IndexError("Duplicated sub-queue name '" + str(name) + "'")
            subtree = self.tree.subtree(matcher, True)
            newPriority = self.queues.setdefault(priority, CBQueue.MultiQueue(self, priority))
            newQueue = CBQueue(subtree, newPriority, maxdefault, maxtotal, defaultQueueClass)
            newPriority.addSubQueue(newQueue)
            qi = [priority, newQueue, name]
            if name is not None:
                self.queueindex[name] = qi
            self.queueindex[newQueue] = qi
            return newQueue
        def removeSubQueue(self, queue):
            '''
            remove a sub queue from current queue.
            
            This unblock the sub-queue, retrieve all events from the queue and put them back to the parent.
            
            Call clear on the sub-queue first if the events are not needed any more.
            
            :param queue: the name or queue object to remove
            
            :returns: ((queueevents,...), (queueEmptyEvents,...)) Possible queue events from removing sub-queues
            '''
            q = self.queueindex[queue]
            q[1].unblockall()
            q[1]._removeFromTree()
            ret = ([],[])
            while q[1].canPop():
                r = q[1].pop()
                self.append(r[0], True)
                ret[0].extend(r[1])
                ret[1].extend(r[2])
            self.queues[q[0]].removeSubQueue(q[1])
            # Remove from index
            if q[2] is not None:
                del self.queueindex[q[2]]
            del self.queueindex[q[1]]
            newblocked =  not self.canPop()
            if newblocked != self.blocked:
                self.blocked = newblocked
                if self.parent is not None:
                    self.parent.notifyBlock(self, newblocked)
            return ret
        def __len__(self):
            return self.totalSize
            
