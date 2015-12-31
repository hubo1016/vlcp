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

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testConsumer']
    unittest.main()
