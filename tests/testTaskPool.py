'''
Created on 2015/12/31

@author: hubo
'''
import unittest
from vlcp.server.server import Server
from vlcp.utils.connector import TaskPool
from vlcp.event.runnable import RoutineContainer
from vlcp.event.event import withIndices, Event

@withIndices()
class TaskTestEvent(Event):
    pass

class Test(unittest.TestCase):


    def setUp(self):
        self.server = Server()
        self.container = RoutineContainer(self.server.scheduler)
        self.taskpool = TaskPool(self.server.scheduler)
        self.taskpool.start()

    def tearDown(self):
        pass


    def testTask(self):
        result = []
        async def routine():
            def t():
                return 1
            result.append(await self.taskpool.run_task(self.container, t))
        self.container.subroutine(routine())
        self.server.serve()
        self.assertEqual(result, [1])
    def testGenTask(self):
        result = []
        result2 = []
        async def routine():
            def t():
                for i in range(0,10):
                    yield (TaskTestEvent(result = i, eof = False),)
                yield (TaskTestEvent(eof = True),)
                return 1
            result2.append(await self.taskpool.run_gen_task(self.container, t))
        async def routine2():
            tte = TaskTestEvent.createMatcher()
            while True:
                ev = await tte
                if ev.eof:
                    break
                else:
                    result.append(ev.result)
        self.container.subroutine(routine())
        self.container.subroutine(routine2())
        self.server.serve()
        self.assertEqual(result, [0,1,2,3,4,5,6,7,8,9])
        self.assertEqual(result2, [1])
    def testAsyncTask(self):
        result = []
        result2 = []
        async def routine():
            def t(sender):
                for i in range(0,10):
                    sender((TaskTestEvent(result = i, eof = False),))
                sender((TaskTestEvent(eof = True),))
                return 1
            result2.append(await self.taskpool.run_async_task(self.container, t))
        async def routine2():
            tte = TaskTestEvent.createMatcher()
            while True:
                ev = await tte
                if ev.eof:
                    break
                else:
                    result.append(ev.result)
        self.container.subroutine(routine())
        self.container.subroutine(routine2())
        self.server.serve()
        self.assertEqual(result, [0,1,2,3,4,5,6,7,8,9])
        self.assertEqual(result2, [1])


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()