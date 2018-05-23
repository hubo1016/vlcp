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



class Test(unittest.TestCase):

    def setUp(self):
        logging.basicConfig()
    def testConsumer(self):
        scheduler = Scheduler()
        scheduler.queue.addSubQueue(10, RoutineControlEvent.createMatcher())
        scheduler.queue.addSubQueue(1, TestConsumerEvent.createMatcher(), 'consumer', 5, 5)
        rA = RoutineContainer(scheduler)
        output = bytearray()
        def mainA():
            rA.subroutine(mainB(), daemon = True)
            for i in range(0,10):
                for ms in rA.waitForSend(TestConsumerEvent(rA.mainroutine)):
                    yield ms
                output.extend(b'A')
        def mainB():
            matcher = TestConsumerEvent.createMatcher(producer=rA.mainroutine)
            while True:
                yield (matcher,)
                rA.event.canignore = True
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
        def mainA():
            rA.subroutine(mainB(), True, 'mainB', True)
            matcher = TestConsumerEvent.createMatcher(rA.mainB)
            for i in range(0,10):
                for ms in rA.waitForSend(TestConsumerEvent(rA.mainroutine)):
                    yield ms
                output.extend(b'A')
                yield (matcher,)
        def mainB():
            matcher = TestConsumerEvent.createMatcher(producer=rA.mainroutine)
            while True:
                yield (matcher,)
                rA.event.canignore = True
                output.extend(b'B')
                for ms in rA.waitForSend(TestConsumerEvent(rA.mainB, canignore = True)):
                    yield ms                
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
        def mainA():
            rA.subroutine(mainB(), daemon = True)
            for i in range(0,10):
                for ms in rA.waitForSend(TestConsumerEvent(rA.mainroutine)):
                    yield ms
                output.extend(b'A')
        def mainB():
            for m in rA.doEvents():
                yield m
            matcher = TestConsumerEvent.createMatcher(producer=rA.mainroutine)
            while True:
                yield (matcher,)
                rA.event.canignore = True
                output.extend(b'B')
        def mainC():
            for m in rA.doEvents():
                yield m
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
        def wait(timeout, append):
            for m in rA.waitWithTimeout(timeout):
                yield m
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
    
    def testTimerThreading(self):
        scheduler = Scheduler()
        rA = RoutineContainer(scheduler)
        exceptions = []
        def test_routine():
            try:
                th = scheduler.setTimer(10)
                threading_result = [None]
                def _cancel(th):
                    try:
                        scheduler.cancelTimer(th)
                        # It should be canceled asynchronously
                        self.assertTrue(th in scheduler.timers)
                    except Exception as e:
                        threading_result[0] = e
                    else:
                        threading_result[0] = True
                t = threading.Thread(target=_cancel, args=(th,))
                t.start()
                for m in rA.waitWithTimeout(0.5):
                    yield m
                t.join(1)
                self.assertFalse(t.isAlive())
                self.assertIs(threading_result[0], True)
                self.assertFalse(th in scheduler.timers)
            except Exception as exc:
                exceptions.append(exc)
                raise
        rA.subroutine(test_routine())
        scheduler.main()
        self.assertFalse(exceptions)
    
    def testLimiter(self):
        def _test_limiter(limit, expected_result, *numbers):
            scheduler = Scheduler()
            rA = RoutineContainer(scheduler)
            rB = RoutineContainer(scheduler)
            limiter = RateLimiter(limit, rA)
            counter = [0]
            result = []
            def _record():
                while True:
                    for m in rA.doEvents():
                        yield m
                    if counter[0] == 0:
                        break
                    result.append(counter[0])
                    counter[0] = 0
            def _limited():
                for m in limiter.limit():
                    yield m
                counter[0] += 1
            def _starter():
                for t in numbers:
                    for _ in range(t):
                        rB.subroutine(_limited())
                    for m in rB.doEvents():
                        yield m
            rA.subroutine(_record(), False)
            rB.subroutine(_starter())
            scheduler.main()
            self.assertEqual(result, expected_result)
        _test_limiter(5, [4], 4)
        _test_limiter(5, [5], 5)
        _test_limiter(5, [5,1], 6)
        _test_limiter(5, [5,4], 9)
        _test_limiter(5, [5,5], 10)
        _test_limiter(5, [5,5,1], 11)
        _test_limiter(1, [1] * 10, 10)
        _test_limiter(5, [4,4,4], 4,4,4)
        _test_limiter(5, [4,5,5,1], 4,6,5)
        _test_limiter(5, [4,5,4,5,5], 4,6,3,5,5)
        _test_limiter(5, [5,1,5], 6,0,5)
        _test_limiter(5, [5,1,5,5,4], 6,0,6,4,4)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testConsumer']
    unittest.main()
