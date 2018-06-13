'''
Created on 2015/6/18

:author: hubo
'''
import unittest
from vlcp.event.core import Scheduler
from vlcp.event.runnable import RoutineContainer, RoutineControlEvent
from vlcp.event.event import Event, withIndices
from vlcp.event.connection import Client, TcpServer
import logging
from time import time
from vlcp.event.ratelimiter import RateLimiter
import threading

@withIndices('producer')
class TestConsumerEvent(Event):
    canignore = False


@withIndices('num')
class TestEvent(Event):
    pass


class Test(unittest.TestCase):

    def setUp(self):
        logging.basicConfig()
    def testConsumer(self):
        scheduler = Scheduler()
        scheduler.queue.addSubQueue(10, RoutineControlEvent.createMatcher())
        scheduler.queue.addSubQueue(1, TestConsumerEvent.createMatcher(), 'consumer', 5, 5)
        rA = RoutineContainer(scheduler)
        output = bytearray()
        async def mainA():
            rA.subroutine(mainB(), daemon = True)
            for _ in range(0,10):
                await rA.waitForSend(TestConsumerEvent(rA.mainroutine))
                output.extend(b'A')
        async def mainB():
            matcher = TestConsumerEvent.createMatcher(producer=rA.mainroutine)
            while True:
                ev = await matcher
                ev.canignore = True
                output.extend(b'B')
        rA.main = mainA
        rA.start()
        scheduler.main()
        self.assertEqual(output, b'AAAAABABABABABABBBBB')
    def testLoopConsumer(self):
        scheduler = Scheduler()
        scheduler.queue.addSubQueue(10, RoutineControlEvent.createMatcher())
        scheduler.queue.addSubQueue(1, TestConsumerEvent.createMatcher(), 'consumer', 5, 5)
        rA = RoutineContainer(scheduler)
        output = bytearray()
        async def mainA():
            rA.subroutine(mainB(), True, 'mainB', True)
            matcher = TestConsumerEvent.createMatcher(rA.mainB)
            for _ in range(0,10):
                await rA.wait_for_send(TestConsumerEvent(rA.mainroutine))
                output.extend(b'A')
                await matcher
        async def mainB():
            matcher = TestConsumerEvent.createMatcher(producer=rA.mainroutine)
            while True:
                ev = await matcher
                ev.canignore = True
                output.extend(b'B')
                await rA.wait_for_send(TestConsumerEvent(rA.mainB, canignore = True))
        rA.main = mainA
        rA.start()
        scheduler.main()
        self.assertEqual(output, b'ABABABABABABABABABAB')
    def testBlock(self):
        scheduler = Scheduler()
        scheduler.queue.addSubQueue(10, RoutineControlEvent.createMatcher())
        scheduler.queue.addSubQueue(1, TestConsumerEvent.createMatcher(), 'consumer', 5, 5)
        rA = RoutineContainer(scheduler)
        output = bytearray()
        async def mainA():
            rA.subroutine(mainB(), daemon = True)
            for _ in range(0,10):
                await rA.wait_for_send(TestConsumerEvent(rA.mainroutine))
                output.extend(b'A')
        async def mainB():
            await rA.do_events()
            matcher = TestConsumerEvent.createMatcher(producer=rA.mainroutine)
            while True:
                ev = await matcher
                ev.canignore = True
                output.extend(b'B')
        async def mainC():
            await rA.do_events()
            output.extend(b'C')
        rA.main = mainA
        rA.start()
        rA.subroutine(mainC())
        scheduler.main()
        self.assertEqual(output, b'AAAAACBABABABABABBBBB')
    def testTimer(self):
        scheduler = Scheduler()
        rA = RoutineContainer(scheduler)
        output = bytearray()
        async def wait(timeout, append):
            await rA.waitWithTimeout(timeout)
            output.extend(append)
        rA.subroutine(wait(0.1, b'B'))
        rA.subroutine(wait(0.5, b'D'))
        rA.subroutine(wait(0, b'A'))
        rA.subroutine(wait(0.2, b'C'))
        curr_time = time()
        scheduler.main()
        end_time = time()
        self.assertEqual(output, b'ABCD')
        self.assertTrue(0.4 < end_time - curr_time < 0.6)
        
    def testLimiter(self):
        def _test_limiter(limit, expected_result, *numbers):
            scheduler = Scheduler()
            rA = RoutineContainer(scheduler)
            rB = RoutineContainer(scheduler)
            limiter = RateLimiter(limit, rA)
            counter = [0, 0]
            result = []
            async def _record():
                first = True
                while True:
                    await rA.do_events()
                    if counter[0] == 0:
                        break
                    result.append((counter[0], counter[1]))
                    counter[0] = 0
                    counter[1] = 0
            async def _limited(use = 1):
                await limiter.limit(use)
                counter[0] += 1
                counter[1] += use
            async def _starter():
                for t in numbers:
                    if isinstance(t, tuple):
                        for use in t:
                            rB.subroutine(_limited(use))
                    else:
                        for _ in range(t):
                            rB.subroutine(_limited())
                    await rB.do_events()
            rA.subroutine(_record(), False)
            rB.subroutine(_starter())
            scheduler.main()
            self.assertEqual(result, expected_result)
        _test_limiter(5, [(4,4)], 4)
        _test_limiter(5, [(5,5)], 5)
        _test_limiter(5, [(5,5),(1,1)], 6)
        _test_limiter(5, [(5,5),(4,4)], 9)
        _test_limiter(5, [(5,5),(5,5)], 10)
        _test_limiter(5, [(5,5),(5,5),(1,1)], 11)
        _test_limiter(1, [(1,1)] * 10, 10)
        _test_limiter(5, [(4,4),(4,4),(4,4)], 4,4,4)
        _test_limiter(5, [(4,4),(5,5),(5,5),(1,1)], 4,6,5)
        _test_limiter(5, [(4,4),(5,5),(4,4),(5,5),(5,5)], 4,6,3,5,5)
        _test_limiter(5, [(5,5),(1,1),(5,5)], 6,0,5)
        _test_limiter(5, [(5,5),(1,1),(5,5),(5,5),(4,4)], 6,0,6,4,4)
        _test_limiter(5, [(1,4)], (4,))
        _test_limiter(5, [(1,5)], (5,))
        _test_limiter(5, [(1,6)], (6,))
        _test_limiter(5, [(2,6)], (3,3))
        _test_limiter(5, [(2,6),(1,3)], (3,3,3))
        _test_limiter(5, [(3,5),(1,3)], (1,3,1,3))
        _test_limiter(5, [(3,7),(1,1)], (3,1,3,1))
        _test_limiter(5, [(3,7),(2,4),(2,4),(3,5)], (3,1,3,1,3,1,3,1,3,1))
        _test_limiter(5, [(2,4),(3,5),(2,6),(1,4),(1,1)], (3,1),(3,1,1,3),(3,4,1))
        _test_limiter(5, [(2,8),(1,4),(1,4),(1,4),(2,8),(1,4),(1,4),(1,4)], (4,)*10)
    
    def testWaitForAll(self):
        scheduler = Scheduler()
        rA = RoutineContainer(scheduler)
        async def sender(num):
            await rA.wait_for_send(TestEvent(num))
        result = []
        async def test():
            try:
                for i in range(10):
                    rA.subroutine(sender(i), False)
                matchers = [TestEvent.createMatcher(i) for i in range(10)]
                _, eventdict = await rA.wait_for_all(*matchers)
                for m in matchers:
                    self.assertIn(m, eventdict)
                    self.assertTrue(m.isMatch(eventdict[m]))
            except Exception as e:
                result.append(e)
                raise
            else:
                result.append(False)
        rA.main = test
        rA.start()
        scheduler.main()
        self.assertIs(result[0], False)
 

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testConsumer']
    unittest.main()
