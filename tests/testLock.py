'''
Created on 2015/12/14

:author: hubo
'''
from __future__ import print_function
import unittest
from vlcp.server.server import Server
from vlcp.event.runnable import RoutineContainer
from vlcp.event.lock import Lock, Semaphore
from vlcp.config.config import manager


class Test(unittest.TestCase):
    def setUp(self):
        self.server = Server()
    def tearDown(self):
        pass
    
    def testLock(self):
        rc = RoutineContainer(self.server.scheduler)
        obj = [0]
        async def routineLock(key):
            l = Lock(key, rc.scheduler)
            await l.lock(rc)
            t = obj[0]
            await rc.wait_with_timeout(0.5)
            obj[0] = t + 1
            l.unlock()
        rc.subroutine(routineLock('testobj'))
        rc.subroutine(routineLock('testobj'))
        self.server.serve()
        self.assertEqual(obj[0], 2)
    def testWith(self):
        rc = RoutineContainer(self.server.scheduler)
        obj = [0]
        async def routineLock(key):
            l = Lock(key, rc.scheduler)
            await l.lock(rc)
            with l:
                t = obj[0]
                await rc.wait_with_timeout(0.5)
                obj[0] = t + 1
        rc.subroutine(routineLock('testobj'))
        rc.subroutine(routineLock('testobj'))
        self.server.serve()
        self.assertEqual(obj[0], 2)
    def testLock2(self):
        rc = RoutineContainer(self.server.scheduler)
        obj = [0]
        async def routineLock(key):
            l = Lock(key, rc.scheduler)
            await l.lock(rc)
            t = obj[0]
            await rc.wait_with_timeout(0.5)
            obj[0] = t + 1
            l.unlock()
        rc.subroutine(routineLock('testobj'))
        rc.subroutine(routineLock('testobj2'))
        self.server.serve()
        self.assertEqual(obj[0], 1)
    def testTrylock(self):
        rc = RoutineContainer(self.server.scheduler)
        result = []
        async def routineTrylock(key):
            l = Lock(key, rc.scheduler)
            locked = l.trylock()
            result.append(locked)
            await rc.wait_with_timeout(0.5)
            l.unlock()
        rc.subroutine(routineTrylock('testobj'))
        rc.subroutine(routineTrylock('testobj'))
        self.server.serve()
        self.assertEqual(result, [True, False])
    def testBeginlock(self):
        rc = RoutineContainer(self.server.scheduler)
        obj = [0]
        async def routineLock(key):
            l = Lock(key, rc.scheduler)
            locked = l.beginlock(rc)
            if not locked:
                await rc.wait_with_timeout(1.0)
                locked = l.trylock()
                if not locked:
                    raise ValueError('Not locked')
            t = obj[0]
            await rc.wait_with_timeout(0.5)
            obj[0] = t + 1
            l.unlock()
            await rc.do_events()
            await l.lock(rc)
            t = obj[0]
            await rc.wait_with_timeout(1.0)
            obj[0] = t + 1
            l.unlock()
        rc.subroutine(routineLock('testobj'))
        rc.subroutine(routineLock('testobj'))
        self.server.serve()
        self.assertEqual(obj[0], 4)
    def testBeginlock2(self):
        rc = RoutineContainer(self.server.scheduler)
        obj = [0]
        async def routineLock(key):
            l = Lock(key, rc.scheduler)
            locked = l.beginlock(rc)
            if not locked:
                await rc.wait_with_timeout(0.5)
                await l.lock(rc)
            t = obj[0]
            await rc.wait_with_timeout(1.0)
            obj[0] = t + 1
            l.unlock()
            await rc.do_events()
            await l.lock(rc)
            t = obj[0]
            if t != 2:
                obj[0] = t - 1
            l.unlock()
        rc.subroutine(routineLock('testobj'))
        rc.subroutine(routineLock('testobj'))
        self.server.serve()
        self.assertEqual(obj[0], 2)
    def testSemaphore(self):
        rc = RoutineContainer(self.server.scheduler)
        obj = [0]
        async def routineLock(key):
            l = Lock(key, rc.scheduler)
            await l.lock(rc)
            t = obj[0]
            await rc.wait_with_timeout(0.5)
            obj[0] = t + 1
            l.unlock()
        async def main_routine():
            smp = Semaphore('testobj', 2, rc.scheduler)
            smp.create()
            await rc.execute_all([routineLock('testobj'),
                                    routineLock('testobj'),
                                    routineLock('testobj'),
                                    routineLock('testobj')])
            await smp.destroy(rc)
        rc.subroutine(main_routine())
        self.server.serve()
        self.assertEqual(obj[0], 2)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()