'''
Created on 2015/6/9

:author: hubo
'''
import unittest
from vlcp.event.event import Event,EventMatcher,withIndices
from vlcp.event.matchtree import MatchTree, EventTree
from vlcp.event.pqueue import CBQueue

@withIndices('type', 'bind', 'number', 'obj')
class TestEvent(Event):
    pass

@withIndices('length')
class TestEvent2(TestEvent):
    pass

@withIndices('type', 'bind', 'number', 'length')
class TestEvent3(Event):
    pass


class Test(unittest.TestCase):


    def setUp(self):
        self.obj = object()
        self.obj2 = object()
        self.tree = MatchTree()
        self.queue = CBQueue(None, None, 5, None)
        self.event1 = TestEvent('READ', ('172.28.98.14',100), 17, self.obj)
        self.event2 = TestEvent('WRITE', ('172.28.0.4',), 17, self.obj2)
        self.event3 = TestEvent('WRITE', ('172.28.0.4',12), 19, self.obj)
        self.event4 = TestEvent('WRITE', ('172.28.0.6',12), 17, self.obj2)
        self.event5 = TestEvent2('WRITE', ('172.28.0.6',12), 17, self.obj2, 27)
        self.event6 = TestEvent3('READ', ('172.28.98.14',100), 17, 27)
        q = self.queue.addSubQueue(1, TestEvent.createMatcher(type='WRITE'), 'write', 15, 20)
        q.addSubQueue(0, TestEvent.createMatcher(bind=('172.28.0.4',)), None, 15, None)
        q = self.queue.addSubQueue(1, TestEvent.createMatcher(type='WRITE', number=19), 'special', 15, 20)
        q = self.queue.addSubQueue(2, TestEvent.createMatcher(type='WRITE', bind=('172.28.98.16',), number=11), 'priority', 15, 15, CBQueue.PriorityQueue)
        q = self.queue.addSubQueue(3, TestEvent.createMatcher(type='WRITE', bind=('172.28.98.17',)), 'autoclass', 15, 15, CBQueue.AutoClassQueue.initHelper('number', 1))
        q = self.queue.addSubQueue(4, TestEvent.createMatcher(type='WRITE', bind=('172.28.98.18',)), 'autoclass2', 15, 15, CBQueue.AutoClassQueue.initHelper('number', 1, subqueuelimit = 5))
        q = self.queue.addSubQueue(4, TestEvent.createMatcher(type='WRITE', bind=('172.28.98.19',)), 'autoclass3', None, None, CBQueue.AutoClassQueue.initHelper('number', 1, subqueuelimit = 5))
        

    def testEventMatcher(self):
        m1 = TestEvent.createMatcher(bind=('172.28.0.4',))
        self.assertFalse(m1.isMatch(self.event1))
        self.assertTrue(m1.isMatch(self.event2))
        self.assertFalse(m1.isMatch(self.event3))
        self.assertFalse(m1.isMatch(self.event4))
        self.assertFalse(m1.isMatch(self.event5))
        self.assertFalse(m1.isMatch(self.event6))
        m2 = TestEvent.createMatcher(type='WRITE', number=17)
        self.assertFalse(m2.isMatch(self.event1))
        self.assertTrue(m2.isMatch(self.event2))
        self.assertFalse(m2.isMatch(self.event3))
        self.assertTrue(m2.isMatch(self.event4))
        self.assertTrue(m2.isMatch(self.event5))
        self.assertFalse(m2.isMatch(self.event6))
        m3 = TestEvent.createMatcher(type='READ', obj=self.obj)
        self.assertTrue(m3.isMatch(self.event1))
        self.assertFalse(m3.isMatch(self.event2))
        self.assertFalse(m3.isMatch(self.event3))
        self.assertFalse(m3.isMatch(self.event4))
        self.assertFalse(m3.isMatch(self.event5))
        self.assertFalse(m3.isMatch(self.event6))
        m4 = TestEvent.createMatcher(type='WRITE',_ismatch=lambda x: x.bind[0] == '172.28.0.4')
        self.assertFalse(m4.isMatch(self.event1))
        self.assertTrue(m4.isMatch(self.event2))
        self.assertTrue(m4.isMatch(self.event3))
        self.assertFalse(m4.isMatch(self.event4))
        self.assertFalse(m4.isMatch(self.event5))
        self.assertFalse(m4.isMatch(self.event6))
        m5 = TestEvent.createMatcher()
        self.assertTrue(m5.isMatch(self.event1))
        self.assertTrue(m5.isMatch(self.event2))
        self.assertTrue(m5.isMatch(self.event3))
        self.assertTrue(m5.isMatch(self.event4))
        self.assertTrue(m5.isMatch(self.event5))
        self.assertFalse(m5.isMatch(self.event6))
        m6 = TestEvent2.createMatcher(type='WRITE', bind=('172.28.0.6',12), obj=self.obj2)
        self.assertFalse(m6.isMatch(self.event1))
        self.assertFalse(m6.isMatch(self.event2))
        self.assertFalse(m6.isMatch(self.event3))
        self.assertFalse(m6.isMatch(self.event4))
        self.assertTrue(m6.isMatch(self.event5))
        self.assertFalse(m6.isMatch(self.event6))
        m7 = TestEvent3.createMatcher(type='READ', number=17)
        self.assertFalse(m7.isMatch(self.event1))
        self.assertFalse(m7.isMatch(self.event2))
        self.assertFalse(m7.isMatch(self.event3))
        self.assertFalse(m7.isMatch(self.event4))
        self.assertFalse(m7.isMatch(self.event5))
        self.assertTrue(m7.isMatch(self.event6))
        m8 = TestEvent2.createMatcher()
        self.assertFalse(m8.isMatch(self.event1))
        self.assertFalse(m8.isMatch(self.event2))
        self.assertFalse(m8.isMatch(self.event3))
        self.assertFalse(m8.isMatch(self.event4))
        self.assertTrue(m8.isMatch(self.event5))
        self.assertFalse(m8.isMatch(self.event6))
        m9 = TestEvent3.createMatcher('READ')
        self.assertFalse(m9.isMatch(self.event1))
        self.assertFalse(m9.isMatch(self.event2))
        self.assertFalse(m9.isMatch(self.event3))
        self.assertFalse(m9.isMatch(self.event4))
        self.assertFalse(m9.isMatch(self.event5))
        self.assertTrue(m9.isMatch(self.event6))
        
    
    def testMatchTree(self):
        self.tree.insert(TestEvent.createMatcher(bind=('172.28.0.4',)), 1)
        self.tree.insert(TestEvent.createMatcher(type='WRITE', number=17), 2)
        self.tree.insert(TestEvent.createMatcher(type='READ', obj=self.obj), 3)
        self.tree.insert(TestEvent.createMatcher(type='WRITE',_ismatch=lambda x: x.bind[0] == '172.28.0.4'), 4)
        self.tree.insert(TestEvent.createMatcher(),5)
        self.tree.insert(TestEvent2.createMatcher(type='WRITE', bind=('172.28.0.6',12), obj=self.obj2), 6)
        self.tree.insert(TestEvent3.createMatcher(type='READ', number=17), 7)
        self.assertEqual(self.tree.matches(self.event1), (3,5))
        self.assertEqual(self.tree.matches(self.event2), (2,4,1,5))
        self.assertEqual(self.tree.matches(self.event3), (4,5))
        self.assertEqual(self.tree.matches(self.event4), (2,5))
        self.assertEqual(self.tree.matches(self.event5), (6,2,5))
        self.assertEqual(self.tree.matches(self.event6), (7,))
        self.tree.remove(TestEvent.createMatcher(type='WRITE',_ismatch=lambda x: x.bind[0] == '172.28.0.4'), 4)
        self.assertEqual(self.tree.matches(self.event1), (3,5))
        self.assertEqual(self.tree.matches(self.event2), (2,1,5))
        self.assertEqual(self.tree.matches(self.event3), (5,))
        self.assertEqual(self.tree.matches(self.event4), (2,5))
        self.assertEqual(self.tree.matches(self.event5), (6,2,5))
        self.assertEqual(self.tree.matches(self.event6), (7,))
        self.tree.insert(TestEvent.createMatcher(type='WRITE',_ismatch=lambda x: x.bind[0] == '172.28.0.4'), 4)
        self.assertEqual(self.tree.matches(self.event1), (3,5))
        self.assertEqual(self.tree.matches(self.event2), (2,4,1,5))
        self.assertEqual(self.tree.matches(self.event3), (4,5))
        self.assertEqual(self.tree.matches(self.event4), (2,5))
        self.assertEqual(self.tree.matches(self.event5), (6,2,5))
        self.assertEqual(self.tree.matches(self.event6), (7,))
    
    @staticmethod
    def popAll(queue):
        while queue.canPop():
            yield queue.pop()
    @staticmethod
    def pushAll(queue, events):
        while events:
            w = queue.append(events[0])
            if w is not None:
                return w
            del events[0]
        return None

    def testQueuePriority(self):
        events = []
        for i in range(0,5):  # @UnusedVariable
            e = TestEvent('READ', ('172.28.0.4',),17,self.obj)
            e.testIndex = len(events)
            events.append(e)
        for i in range(0,5):  # @UnusedVariable
            e = TestEvent('WRITE', ('172.28.0.6',),17,self.obj)
            e.testIndex = len(events)
            events.append(e)
        for i in range(0,5):  # @UnusedVariable
            e = TestEvent('WRITE', ('172.28.0.4',),17,self.obj)
            e.testIndex = len(events)
            events.append(e)
        for i in range(0,5):  # @UnusedVariable
            e = TestEvent('WRITE', ('172.28.0.4',),19,self.obj)
            e.testIndex = len(events)
            events.append(e)
        for i in range(0,5):  # @UnusedVariable
            e = TestEvent('WRITE', ('172.28.0.6',),19,self.obj)
            e.testIndex = len(events)
            events.append(e)
        for i in range(0,5):
            e = TestEvent('WRITE', ('172.28.98.16',),11,self.obj, priority = 10 - (i * 2) % 5)
            e.testIndex = len(events)
            events.append(e)
        ans = self.pushAll(self.queue, events)
        self.assertTrue(not events and ans is None)
        eventOut = [e[0].testIndex for e in self.popAll(self.queue)]
        self.assertEqual(eventOut, [27,29,26,28,25,5,20,10,21,6,22,11,23,7,24,12,8,13,9,14,15,16,17,18,19,0,1,2,3,4])
    
    def testQueueFull(self):
        events = []
        for i in range(0,20):  # @UnusedVariable
            e = TestEvent('WRITE', ('172.28.0.6',),17,self.obj)
            e.testIndex = len(events)
            events.append(e)
        ans = self.pushAll(self.queue, events)
        self.assertEqual(len(events), 5)
        self.assertTrue(ans is not None and isinstance(ans, EventMatcher) and ans.indices[1] is self.queue['write'].defaultQueue)
        (result, we, es) = self.queue.pop()
        self.assertEqual(result.testIndex, 0)
        self.assertEqual(len(we), 1)
        self.assertTrue(we[0].queue is self.queue['write'].defaultQueue)
        self.assertTrue(ans.isMatch(we[0]))
        ans = self.pushAll(self.queue, events)
        self.assertEqual(len(events), 4)
        self.assertTrue(ans is not None and isinstance(ans, EventMatcher))
        events = []
        for i in range(0,20):  # @UnusedVariable
            e = TestEvent('WRITE', ('172.28.0.4',), 17, self.obj)
            e.testIndex = len(events) + 20
            events.append(e)
        ans2 = self.pushAll(self.queue, events)
        self.assertEqual(len(events), 15)
        self.assertTrue(ans is not None and isinstance(ans, EventMatcher))
        (result, we, es) = self.queue.pop()
        self.assertTrue(len(we) >= 1)
        self.assertTrue(ans2.isMatch(we[0]) or ans2.isMatch(we[1]))
        events = []
        for i in range(0,20):  # @UnusedVariable
            e = TestEvent('WRITE', ('172.28.98.16',), 11, self.obj, priority = (i * 4) % 21)
            e.testIndex = len(events) + 40
            events.append(e)
        ans = self.pushAll(self.queue, events)
        self.assertEqual(len(events), 5)
        self.assertTrue(ans is not None and isinstance(ans, EventMatcher))
        (result, we, es) = self.queue.pop()
        self.assertEqual(len(we), 1)
        self.assertTrue(ans.isMatch(we[0]))
        
    
    def testQueueBlock(self):
        events = []
        for i in range(0,5):  # @UnusedVariable
            e = TestEvent('READ', ('172.28.0.4',),17,self.obj)
            e.testIndex = len(events)
            events.append(e)
        for i in range(0,5):  # @UnusedVariable
            e = TestEvent('WRITE', ('172.28.0.6',),17,self.obj)
            e.testIndex = len(events)
            events.append(e)
        for i in range(0,5):  # @UnusedVariable
            e = TestEvent('WRITE', ('172.28.0.4',),17,self.obj)
            e.testIndex = len(events)
            events.append(e)
        ans = self.pushAll(self.queue, events)
        self.assertTrue(not events and ans is None)
        (ret, ws, es) = self.queue.pop()
        self.assertTrue(not ws)
        self.assertEqual(ret.testIndex, 5)
        self.queue.block(ret)
        eventOut = [e[0].testIndex for e in self.popAll(self.queue)]
        self.assertEqual(eventOut, [10,11,12,13,14,0,1,2,3,4])
        self.queue.unblock(ret)
        eventOut = [e[0].testIndex for e in self.popAll(self.queue)]
        self.assertEqual(eventOut, [5,6,7,8,9])
        events = []
        for i in range(0,5):  # @UnusedVariable
            e = TestEvent('READ', ('172.28.0.4',),17,self.obj)
            e.testIndex = len(events)
            events.append(e)
        for i in range(0,5):  # @UnusedVariable
            e = TestEvent('WRITE', ('172.28.0.6',),17,self.obj)
            e.testIndex = len(events)
            events.append(e)
        ans = self.pushAll(self.queue, events)
        self.assertTrue(not events and ans is None)
        (ret, ws, es) = self.queue.pop()
        self.assertTrue(not ws)
        self.assertEqual(ret.testIndex, 5)
        self.queue.block(ret)
        (ret2, ws, es) = self.queue.pop()
        self.assertTrue(not ws)
        self.assertEqual(ret2.testIndex, 0)
        self.queue.block(ret2)
        self.assertFalse(self.queue.canPop())
        self.assertEqual(len(self.queue), 10)
        for i in range(0,5):  # @UnusedVariable
            e = TestEvent('WRITE', ('172.28.0.6',),17,self.obj)
            e.testIndex = len(events) + 10
            events.append(e)
        ans = self.pushAll(self.queue, events)
        self.assertTrue(not events and ans is None)        
        self.assertFalse(self.queue.canPop())
        self.queue.unblockall()
        eventOut = [e[0].testIndex for e in self.popAll(self.queue)]
        self.assertEqual(eventOut, [5,6,7,8,9,10,11,12,13,14,0,1,2,3,4])
        events = []
        for i in range(0,5):
            e = TestEvent('WRITE', ('172.28.98.16',),11,self.obj, priority = 10 - (i * 2) % 5)
            e.testIndex = len(events)
            events.append(e)
        ans = self.pushAll(self.queue, events)
        self.assertTrue(not events and ans is None)
        (ret, ws, es) = self.queue.pop()
        self.assertTrue(not ws)
        self.assertEqual(ret.testIndex, 2)
        self.queue.block(ret)
        self.assertFalse(self.queue.canPop())
        for i in range(0,5):
            e = TestEvent('WRITE', ('172.28.98.16',),11,self.obj, priority = 5 - (i * 2) % 5)
            e.testIndex = len(events) + 5
            events.append(e)
        ans = self.pushAll(self.queue, events)
        self.assertTrue(not events and ans is None)
        (ret2, ws, es) = self.queue.pop()
        self.assertTrue(not ws)
        self.assertEqual(ret2.testIndex, 7)
        self.queue.block(ret2)
        self.assertFalse(self.queue.canPop())
        for i in range(0,5):
            e = TestEvent('WRITE', ('172.28.98.16',),11,self.obj, priority = - ((i * 2) % 5))
            e.testIndex = len(events) + 10
            events.append(e)
        ans = self.pushAll(self.queue, events)
        self.assertTrue(not events and ans is None)
        eventOut = [e[0].testIndex for e in self.popAll(self.queue)]
        self.assertEqual(eventOut, [12,14,11,13,10])
        self.queue.unblock(ret)
        self.assertFalse(self.queue.canPop())
        self.queue.unblock(ret2)
        eventOut = [e[0].testIndex for e in self.popAll(self.queue)]
        self.assertEqual(eventOut, [7,9,6,8,5,2,4,1,3,0])
        
        
    def testClear(self):
        events = []
        for i in range(0,5):  # @UnusedVariable
            e = TestEvent('READ', ('172.28.0.4',),17,self.obj)
            e.testIndex = len(events)
            events.append(e)
        for i in range(0,5):  # @UnusedVariable
            e = TestEvent('WRITE', ('172.28.0.6',),17,self.obj)
            e.testIndex = len(events)
            events.append(e)
        for i in range(0,5):  # @UnusedVariable
            e = TestEvent('WRITE', ('172.28.0.4',),17,self.obj)
            e.testIndex = len(events)
            events.append(e)
        for i in range(0,5):  # @UnusedVariable
            e = TestEvent('WRITE', ('172.28.0.4',),19,self.obj)
            e.testIndex = len(events)
            events.append(e)
        for i in range(0,5):  # @UnusedVariable
            e = TestEvent('WRITE', ('172.28.0.6',),19,self.obj)
            e.testIndex = len(events)
            events.append(e)
        for i in range(0,5):
            e = TestEvent('WRITE', ('172.28.98.16',),11,self.obj, priority = 10 - (i * 2) % 5)
            e.testIndex = len(events)
            events.append(e)
        ans = self.pushAll(self.queue, events)
        self.assertTrue(not events and ans is None)
        (ret, ws, es) = self.queue.pop()
        self.assertTrue(not ws)
        self.assertEqual(ret.testIndex, 27)
        self.queue.block(ret)
        self.queue.clear()
        self.assertEqual(len(self.queue), 0)
        self.assertFalse(self.queue.canPop())
        self.assertFalse(self.queue.blockEvents)
    
    def testCircleList(self):
        cl = CBQueue.MultiQueue.CircleList()
        n = CBQueue.MultiQueue.CircleListNode(1)
        n2 = CBQueue.MultiQueue.CircleListNode(2)
        n3 = CBQueue.MultiQueue.CircleListNode(3)
        cl.insertprev(n)
        self.assertTrue(cl.current is n)
        cl.insertprev(n2)
        self.assertTrue(cl.current is n)
        cl.remove(n)
        self.assertTrue(cl.current is n2)
        cl.insertprev(n3)
        self.assertTrue(cl.next() is n2)
        self.assertTrue(cl.next() is n3)
        self.assertTrue(cl.next() is n2)
        self.assertTrue(cl.current is n3)
        cl.remove(n2)
        self.assertTrue(cl.current is n3)
        cl.clear()
        self.assertTrue(cl.current is None)
        
    def testRemoveQueue(self):
        events = []
        for i in range(0,5):  # @UnusedVariable
            e = TestEvent('READ', ('172.28.0.4',),17,self.obj)
            e.testIndex = len(events)
            events.append(e)
        for i in range(0,5):  # @UnusedVariable
            e = TestEvent('WRITE', ('172.28.0.6',),17,self.obj)
            e.testIndex = len(events)
            events.append(e)
        ans = self.pushAll(self.queue, events)
        self.assertTrue(not events and ans is None)
        self.queue.removeSubQueue('write')
        eventOut = [e[0].testIndex for e in self.popAll(self.queue)]
        self.assertEqual(eventOut, [0,1,2,3,4,5,6,7,8,9])
    
    def testSetPriority(self):
        events = []
        for i in range(0,5):  # @UnusedVariable
            e = TestEvent('READ', ('172.28.0.4',),17,self.obj)
            e.testIndex = len(events)
            events.append(e)
        for i in range(0,5):  # @UnusedVariable
            e = TestEvent('WRITE', ('172.28.0.6',),17,self.obj)
            e.testIndex = len(events)
            events.append(e)
        for i in range(0,5):  # @UnusedVariable
            e = TestEvent('WRITE', ('172.28.0.6',),19,self.obj)
            e.testIndex = len(events)
            events.append(e)
        ans = self.pushAll(self.queue, events)
        self.assertTrue(not events and ans is None)
        self.queue.setPriority('special', 3)
        eventOut = [e[0].testIndex for e in self.popAll(self.queue)]
        self.assertEqual(eventOut, [10,11,12,13,14,5,6,7,8,9,0,1,2,3,4])
    
    def testEventTree(self):
        def createTree():
            tree = EventTree()
            tree.insert(self.event1)
            tree.insert(self.event2)
            tree.insert(self.event3)
            tree.insert(self.event4)
            tree.insert(self.event5)
            tree.insert(self.event6)
            return tree
        t = createTree()
        self.assertTrue(hasattr(t,'index'))
        self.assertEqual(t.findAndRemove(TestEvent.createMatcher(bind=('172.28.0.4',))), (self.event2,))
        self.assertEqual(t.findAndRemove(TestEvent.createMatcher(bind=('172.28.0.4',))), ())
        t = createTree()
        self.assertEqual(t.findAndRemove(TestEvent.createMatcher(type='WRITE', number=17)), (self.event2, self.event4, self.event5))
        self.assertEqual(t.findAndRemove(TestEvent.createMatcher(type='WRITE', number=17)), ())
        self.assertEqual(t.findAndRemove(TestEvent.createMatcher(type='READ', obj=self.obj)), (self.event1,))
    
    def testEmptyEvent(self):
        self.queue.append(self.event1)
        m = self.queue.waitForEmpty()
        (ret, ws, es) = self.queue.pop()
        self.assertEqual(len(es), 1)
        self.assertTrue(m.isMatch(es[0]))
        self.queue.block(ret, es)
        self.queue.unblock(ret)
        (ret, ws, es) = self.queue.pop()
        self.assertEqual(len(es), 1)
        self.assertTrue(m.isMatch(es[0]))
    
    def testAutoClassQueue(self):
        events = []
        for i in range(0, 20):
            events.append(TestEvent('WRITE', ('172.28.98.17',), 10, self.obj, testid = i))
        ans = self.pushAll(self.queue, events)
        self.assertEqual(len(events), 6)
        events = []
        for i in range(0, 20):
            events.append(TestEvent('WRITE', ('172.28.98.17',), 20, self.obj, testid = i + 20))
        ans2 = self.pushAll(self.queue, events)
        self.assertEqual(len(events), 19)
        (ret, ws, es) = self.queue.pop()
        self.assertFalse(ws)
        self.assertEqual(ret.testid, 0)
        ans3 = self.queue.append(TestEvent('WRITE', ('172.28.98.17',), 30, self.obj2, testid = 40))
        self.assertTrue(ans3 is None)
        (ret, ws, es) = self.queue.pop()
        self.assertEqual(ret.testid, 40)
        self.assertFalse(ws)
        ans4 = self.queue.append(TestEvent('WRITE', ('172.28.98.17',), 30, self.obj2, testid = 41))
        self.assertTrue(ans4 is not None)
        (ret, ws, es) = self.queue.pop()
        self.assertEqual(ret.testid, 20)
        self.assertEqual(len(ws), 1)
        self.assertTrue(ans2.isMatch(ws[0]))
        self.assertFalse(ans.isMatch(ws[0]))
        self.assertTrue(ans4.isMatch(ws[0]))
        (ret, ws, es) = self.queue.pop()
        self.assertEqual(ret.testid, 1)
        self.assertFalse(ws)
        (ret, ws, es) = self.queue.pop()
        self.assertEqual(ret.testid, 2)
        self.assertEqual(len(ws), 1)
        self.assertTrue(ans.isMatch(ws[0]))
        blk = ret
        self.queue.block(ret, es)
        ans2 = self.pushAll(self.queue, events)
        self.assertEqual(len(events), 18)
        ans3 = self.queue.append(TestEvent('WRITE', ('172.28.98.17',), 30, self.obj2, testid = 42))
        self.assertTrue(ans3 is None)
        ans4 = self.queue.append(TestEvent('WRITE', ('172.28.98.17',), 30, self.obj2, testid = 43))
        self.assertTrue(ans4 is not None)
        (ret, ws, es) = self.queue.pop()
        self.assertEqual(ret.testid, 42)
        self.assertEqual(len(ws), 1)
        self.assertTrue(ans4.isMatch(ws[0]) and not ans2.isMatch(ws[0]) and not ans.isMatch(ws[0]))
        self.queue.unblock(blk)
        (ret, ws, es) = self.queue.pop()
        self.assertEqual(ret.testid, 21)
        self.assertEqual(len(ws), 1)
        self.assertTrue(not ans4.isMatch(ws[0]) and ans2.isMatch(ws[0]) and not ans.isMatch(ws[0]))
    def testAutoClassQueueWithSubQueueLimit(self):
        events = []
        for i in range(0, 20):
            events.append(TestEvent('WRITE', ('172.28.98.18',), 10, self.obj, testid = i))
        ans = self.pushAll(self.queue, events)
        self.assertEqual(len(events), 15)
        events = []
        for i in range(0, 20):
            events.append(TestEvent('WRITE', ('172.28.98.18',), 20, self.obj, testid = i + 20))
        ans2 = self.pushAll(self.queue, events)
        self.assertEqual(len(events), 15)
        (ret, ws, es) = self.queue.pop()
        self.assertEqual(len(ws), 1)
        self.assertTrue(ans.isMatch(ws[0]))
        self.assertFalse(ans2.isMatch(ws[0]))
        self.assertEqual(ret.testid, 0)
        events = []
        for i in range(0, 20):
            events.append(TestEvent('WRITE', ('172.28.98.18',), 30, self.obj, testid = i + 40))
        ans3 = self.pushAll(self.queue, events)
        self.assertEqual(len(events), 15)
        (ret, ws, es) = self.queue.pop()
        self.assertEqual(ret.testid, 40)
        self.assertEqual(len(ws), 1)
        self.assertFalse(ans.isMatch(ws[0]))
        self.assertFalse(ans2.isMatch(ws[0]))
        self.assertTrue(ans3.isMatch(ws[0]))
        ans3 = self.pushAll(self.queue, events)
        self.assertEqual(len(events), 14)
        events = []
        for i in range(0, 20):
            events.append(TestEvent('WRITE', ('172.28.98.18',), 40, self.obj, testid = i + 60))
        ans4 = self.pushAll(self.queue, events)
        self.assertEqual(len(events), 19)
        (ret, ws, es) = self.queue.pop()
        self.assertEqual(ret.testid, 60)
        self.assertEqual(len(ws), 1)
        self.assertFalse(ans.isMatch(ws[0]))
        self.assertFalse(ans2.isMatch(ws[0]))
        self.assertFalse(ans3.isMatch(ws[0]))
        self.assertTrue(ans4.isMatch(ws[0]))        
        ans4 = self.pushAll(self.queue, events)
        self.assertEqual(len(events), 19)
        (ret, ws, es) = self.queue.pop()
        self.assertEqual(ret.testid, 20)
        self.assertEqual(len(ws), 1)
        self.assertFalse(ans.isMatch(ws[0]))
        self.assertTrue(ans2.isMatch(ws[0]))
        self.assertFalse(ans3.isMatch(ws[0]))
        self.assertTrue(ans4.isMatch(ws[0]))
        (ws, es) = self.queue.clear()
        self.assertEqual(len(ws), 1)
        self.assertTrue(ans.isMatch(ws[0]))
        self.assertTrue(ans2.isMatch(ws[0]))
        self.assertTrue(ans3.isMatch(ws[0]))
        self.assertTrue(ans4.isMatch(ws[0]))
    def testAutoClassQueueWithoutMainLimit(self):
        events = []
        for i in range(0, 20):
            events.append(TestEvent('WRITE', ('172.28.98.19',), 10, self.obj, testid = i))
        ans = self.pushAll(self.queue, events)
        self.assertEqual(len(events), 15)
        events = []
        for i in range(0, 20):
            events.append(TestEvent('WRITE', ('172.28.98.19',), 20, self.obj, testid = i + 20))
        ans2 = self.pushAll(self.queue, events)
        self.assertEqual(len(events), 15)
        (ret, ws, es) = self.queue.pop()
        self.assertEqual(len(ws), 1)
        self.assertTrue(ans.isMatch(ws[0]))
        self.assertFalse(ans2.isMatch(ws[0]))
        self.assertEqual(ret.testid, 0)
        events = []
        for i in range(0, 20):
            events.append(TestEvent('WRITE', ('172.28.98.19',), 30, self.obj, testid = i + 40))
        ans3 = self.pushAll(self.queue, events)
        self.assertEqual(len(events), 15)
        (ret, ws, es) = self.queue.pop()
        self.assertEqual(ret.testid, 40)
        self.assertEqual(len(ws), 1)
        self.assertFalse(ans.isMatch(ws[0]))
        self.assertFalse(ans2.isMatch(ws[0]))
        self.assertTrue(ans3.isMatch(ws[0]))
        ans3 = self.pushAll(self.queue, events)
        self.assertEqual(len(events), 14)
        events = []
        for i in range(0, 20):
            events.append(TestEvent('WRITE', ('172.28.98.19',), 40, self.obj, testid = i + 60))
        ans4 = self.pushAll(self.queue, events)
        self.assertEqual(len(events), 15)
        (ret, ws, es) = self.queue.pop()
        self.assertEqual(ret.testid, 60)
        self.assertEqual(len(ws), 1)
        self.assertFalse(ans.isMatch(ws[0]))
        self.assertFalse(ans2.isMatch(ws[0]))
        self.assertFalse(ans3.isMatch(ws[0]))
        self.assertTrue(ans4.isMatch(ws[0]))        
        ans4 = self.pushAll(self.queue, events)
        self.assertEqual(len(events), 14)
        (ret, ws, es) = self.queue.pop()
        self.assertEqual(ret.testid, 20)
        self.assertEqual(len(ws), 1)
        self.assertFalse(ans.isMatch(ws[0]))
        self.assertTrue(ans2.isMatch(ws[0]))
        self.assertFalse(ans3.isMatch(ws[0]))
        self.assertFalse(ans4.isMatch(ws[0]))
        (ws, es) = self.queue.clear()
        self.assertEqual(len(ws), 1)
        self.assertTrue(ans.isMatch(ws[0]))
        self.assertTrue(ans2.isMatch(ws[0]))
        self.assertTrue(ans3.isMatch(ws[0]))
        self.assertTrue(ans4.isMatch(ws[0]))

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testEvent']
    unittest.main()

